library(synapser)
library(arrow)
library(dplyr)
library(synapserutils)
library(rjson)

synapser::synLogin(authToken = Sys.getenv('SYNAPSE_AUTH_TOKEN'))
source('~/recover-parquet-external/params.R')

#### Get STS token for bucket in order to sync to local dir ####

# Get STS credentials
token <- synapser::synGetStsStorageToken(
  entity = PARQUET_FOLDER_INTERNAL,
  permission = "read_only",
  output_format = "json")

if (PARQUET_BUCKET==token$bucket && PARQUET_BUCKET_BASE_KEY==token$baseKey) {
  base_s3_uri <- paste0('s3://', token$bucket, '/', token$baseKey)
} else {
  base_s3_uri <- paste0('s3://', PARQUET_BUCKET, '/', PARQUET_BUCKET_BASE_KEY)
}

base_s3_uri_external <- paste0('s3://', PARQUET_BUCKET_EXTERNAL, '/', PARQUET_BUCKET_BASE_KEY_EXTERNAL)
base_s3_uri_archive <- paste0('s3://', PARQUET_BUCKET_EXTERNAL, '/', PARQUET_BUCKET_BASE_KEY_ARCHIVE)

# configure the environment with AWS token
Sys.setenv('AWS_ACCESS_KEY_ID'=token$accessKeyId,
           'AWS_SECRET_ACCESS_KEY'=token$secretAccessKey,
           'AWS_SESSION_TOKEN'=token$sessionToken)

#### Sync bucket to local dir####
unlink(AWS_PARQUET_DOWNLOAD_LOCATION, recursive = T, force = T)
sync_cmd <- glue::glue('aws s3 sync {base_s3_uri} {AWS_PARQUET_DOWNLOAD_LOCATION} --exclude "*owner.txt*" --exclude "*archive*"')
system(sync_cmd)


# Filter parquet datasets -------------------------------------------------
source('~/recover-parquet-external/filtering.R')

# Copy unfiltered parquet datasets to new location with filtered parquet datasets
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

unlink(PARQUET_FINAL_LOCATION, recursive = T, force = T)
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

      cat("Copied:", folder, '\n')
    } else {
      cat("Skipped:", folder, "- Folder already exists in", destination_folder, '\n')
    }
  }
}

copy_folders_reparent(AWS_PARQUET_DOWNLOAD_LOCATION, PARQUET_FINAL_LOCATION)

# Remove intermediate folders
unlink(PARQUET_FILTERED_LOCATION, recursive = T, force = T)


# De-identify parquet datasets --------------------------------------------
source('~/recover-parquet-external/deidentification.R')

# Sync final parquets to bucket -------------------------------------------

date <- lubridate::today()
sync_cmd <- glue::glue('aws s3 --profile service-catalog sync {PARQUET_FINAL_LOCATION} {base_s3_uri_archive}{date}/ --exclude "*owner.txt*" --exclude "*archive*"')
system(sync_cmd)


# Upload parquet datasets directory tree to Synapse ------------------------

# existing_dirs <- synGetChildren(PARQUET_FOLDER_ARCHIVE) %>% as.list()
# 
# if(length(existing_dirs)>0) {
#   for (i in seq_along(existing_dirs)) {
#     synDelete(existing_dirs[[i]]$id)
#   }
# }

# Modify cohort identifier in dir name
replace_equal_with_underscore <- function(directory_path) {
  new_directory_path <- gsub("=", "_", directory_path)
  if (directory_path != new_directory_path) {
    file.rename(directory_path, new_directory_path)
    cat("Renamed:", directory_path, "to", new_directory_path, "\n")
  }
}

# Generate manifest of existing files
sync_cmd <- glue::glue('aws s3 --profile service-catalog sync {base_s3_uri_archive} {AWS_ARCHIVE_DOWNLOAD_LOCATION} --exclude "*owner.txt*" --exclude "*archive*"')
system(sync_cmd)

invisible(lapply(list.dirs(AWS_ARCHIVE_DOWNLOAD_LOCATION), replace_equal_with_underscore))

SYNAPSE_AUTH_TOKEN <- Sys.getenv('SYNAPSE_AUTH_TOKEN')
manifest_cmd <- glue::glue('SYNAPSE_AUTH_TOKEN="{SYNAPSE_AUTH_TOKEN}" synapse manifest --parent-id {PARQUET_FOLDER_ARCHIVE} --manifest ./current_manifest.tsv {AWS_ARCHIVE_DOWNLOAD_LOCATION}')
system(manifest_cmd)

