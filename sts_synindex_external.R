library(synapser)
library(arrow)
library(dplyr)
library(synapserutils)
library(rjson)

synapser::synLogin(authToken = Sys.getenv('SYNAPSE_AUTH_TOKEN'))
source('~/recover-sts-synindex/sts_params_external.R')

#### Get STS token for bucket in order to sync to local dir ####

# Get STS credentials
token <- synapser::synGetStsStorageToken(
  entity = PARQUET_INTERNAL_FOLDER,
  permission = "read_only",
  output_format = "json")

if (PARQUET_BUCKET==token$bucket && PARQUET_BUCKET_BASE_KEY==token$baseKey) {
  base_s3_uri <- paste0('s3://', token$bucket, '/', token$baseKey)
} else {
  base_s3_uri <- paste0('s3://', PARQUET_BUCKET, '/', PARQUET_BUCKET_BASE_KEY)
}

# configure the environment with AWS token
Sys.setenv('AWS_ACCESS_KEY_ID'=token$accessKeyId,
           'AWS_SECRET_ACCESS_KEY'=token$secretAccessKey,
           'AWS_SESSION_TOKEN'=token$sessionToken)

#### Sync bucket to local dir####
sync_cmd <- glue::glue('aws s3 sync {base_s3_uri} {AWS_PARQUET_DOWNLOAD_LOCATION} --exclude "*owner.txt*" --exclude "*archive*"')
system(sync_cmd)


# Filter parquet datasets -------------------------------------------------

source('~/recover-parquet-external/filtering.R')

### Copy unfiltered parquet datasets to new location with filtered parquet datasets ####
duplicate_folder <- function(source_folder, destination_folder) {
  if (dir.exists(source_folder)) {
    if (!dir.exists(destination_folder)) {
      dir.create(destination_folder)
    } else {
      warning("Destination folder already exists. Files might be overwritten.")
    }
    
    system(glue::glue('cp -r {source_folder}/* {destination_folder}'))
    
    return(destination_folder)
  } else {
    stop("Source folder does not exist.")
  }
}

duplicate_folder(source_folder = PARQUET_FILTERED_LOCATION, 
                 destination_folder = PARQUET_FINAL_LOCATION)

copy_folders_reparent <- function(source_folder, destination_folder) {
  folders_to_copy <- setdiff(list.dirs(source_folder, recursive = F, full.names = F), 
                             list.dirs(destination_folder, recursive = F, full.names = F))
  
  for (folder in folders_to_copy) {
    source_path <- paste0(AWS_PARQUET_DOWNLOAD_LOCATION, '/', folder)
    dest_path <- paste0(PARQUET_FINAL_LOCATION, '/', folder)

    if (!dir.exists(dest_path)) {
      system(glue::glue('cp -r {source_path} {destination_folder}'))

      print(paste0("Copied: ", folder))
    } else {
      print(paste0("Skipped: ", folder, " (Folder already exists in ", destination_folder, ")"))
    }
  }
}

copy_folders_reparent(AWS_PARQUET_DOWNLOAD_LOCATION, PARQUET_FINAL_LOCATION)

#### Store Filtered Datasets in Synapse ####

existing_dirs <- synGetChildren(SYNAPSE_PARENT_ID) %>% as.list()

if(length(existing_dirs)>0) {
  for (i in seq_along(existing_dirs)) {
    synDelete(existing_dirs[[i]]$id)
  }
}

# Generate manifest of existing files
SYNAPSE_AUTH_TOKEN <- Sys.getenv('SYNAPSE_AUTH_TOKEN')
manifest_cmd <- glue::glue('SYNAPSE_AUTH_TOKEN="{SYNAPSE_AUTH_TOKEN}" synapse manifest --parent-id {SYNAPSE_PARENT_ID} --manifest ./current_manifest.tsv {PARQUET_FINAL_LOCATION}')
system(manifest_cmd)

## Get a list of all files to upload and their synapse locations(parentId) 
STR_LEN_PARQUET_FINAL_LOCATION <- stringr::str_length(PARQUET_FINAL_LOCATION)

## All files present locally from manifest
synapse_manifest <- read.csv('./current_manifest.tsv', sep = '\t', stringsAsFactors = F) %>% 
  dplyr::filter(path != paste0(PARQUET_FINAL_LOCATION,'/owner.txt')) %>%  # need not create a dataFileHandleId for owner.txt
  dplyr::rowwise() %>% 
  dplyr::mutate(file_key = stringr::str_sub(string = path, start = STR_LEN_PARQUET_FINAL_LOCATION+2)) %>% # location of file from home folder of S3 bucket
  dplyr::mutate(s3_file_key = paste0(file_key)) %>% # the namespace for files in the S3 bucket is S3::bucket/main/
  dplyr::mutate(md5_hash = as.character(tools::md5sum(path))) %>% 
  dplyr::ungroup()

## All currently indexed files in Synapse
synapse_fileview <- synapser::synTableQuery(paste0('SELECT * FROM ', SYNAPSE_FILEVIEW_ID))$filepath %>% read.csv()

## find those files that are not in the fileview - files that need to be indexed
if (nrow(synapse_fileview)>0) {
  synapse_manifest_to_upload <- 
    synapse_manifest %>% 
    dplyr::anti_join(
      synapse_fileview %>% 
        dplyr::select(parent = parentId,
                      s3_file_key = dataFileKey,
                      md5_hash = dataFileMD5Hex))
  } else {
    synapse_manifest_to_upload <- synapse_manifest
  }

# Upload the contents of the parquet_final folder to Synapse
for (i in seq_along(synapse_manifest_to_upload$path)) {
  synStore(File(synapse_manifest_to_upload$path[i], parent=synapse_manifest_to_upload$parent[i]))
}
