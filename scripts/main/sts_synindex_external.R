library(synapser)
library(arrow)
library(dplyr)


# Functions ---------------------------------------------------------------

#' Duplicate a folder
#'
#' This function duplicates a folder from the source to the destination. It checks
#' if the source folder exists, and if the destination folder already exists, it
#' gives a warning about possible file overwriting.
#'
#' @param source_folder The path to the source folder.
#' @param destination_folder The path to the destination folder.
#'
#' @return The path to the destination folder.
#'
#' @examples
#' source_folder <- "path/to/source_folder"
#' destination_folder <- "path/to/destination_folder"
#' duplicate_folder(source_folder, destination_folder)
#' 
duplicate_folder <- function(source_folder, destination_folder) {
  if (!dir.exists(source_folder)) {
    stop("Source folder does not exist.")
  }
  
  if (dir.exists(destination_folder)) {
    warning("Destination folder already exists. Files might be overwritten.")
  } else {
    dir.create(destination_folder)
  }
  
  system(glue::glue('cp -r {source_folder}/* {destination_folder}'))
  
  return(destination_folder)
}

#' Copy folders and reparent
#'
#' This function copies folders from the source folder to the destination folder
#' while re-parenting them. It skips folders that already exist in the destination folder.
#'
#' @param source_folder The path to the source folder.
#' @param destination_folder The path to the destination folder.
#'
#' @examples
#' source_folder <- "path/to/source_folder"
#' destination_folder <- "path/to/destination_folder"
#' copy_folders_reparent(source_folder, destination_folder)
#' 
copy_folders_reparent <- function(source_folder, destination_folder) {
  folders_to_copy <- 
    setdiff(
      list.dirs(source_folder, recursive = F, full.names = F), 
      list.dirs(destination_folder, recursive = F, full.names = F))
  
  for (folder in folders_to_copy) {
    source_path <- paste0(source_folder, '/', folder)
    dest_path <- paste0(destination_folder, '/', folder)
    
    if (!dir.exists(dest_path)) {
      system(glue::glue('cp -r {source_path} {destination_folder}'))
      cat("Copied:", folder, '\n')
    } else {
      cat("Skipped:", folder, "- Folder already exists in", destination_folder, '\n')
    }
  }
}

#' Replace equal sign with underscore
#'
#' This function renames a directory path by replacing equal signs with underscores.
#' If a replacement is performed, it logs the change.
#'
#' @param directory_path The path of the directory to rename.
#'
#' @examples
#' replace_equal_with_underscore("path_with=equals")
#' 
replace_equal_with_underscore <- function(directory_path) {
  new_directory_path <- gsub("=", "_", directory_path)
  if (directory_path != new_directory_path) {
    file.rename(directory_path, new_directory_path)
    return(cat("Renamed:", directory_path, "to", new_directory_path, "\n"))
  }
}

# Setup -------------------------------------------------------------------
synapser::synLogin(authToken = Sys.getenv('SYNAPSE_AUTH_TOKEN'))
config::get(config = "staging") %>% list2env(envir = .GlobalEnv)


# Get STS credentials for input data bucket -------------------------------
token <- 
  synapser::synGetStsStorageToken(
    entity = PARQUET_FOLDER_INTERNAL,
    permission = "read_only",
    output_format = "json")

if (PARQUET_BUCKET==token$bucket && PARQUET_BUCKET_BASE_KEY==token$baseKey) {
  base_s3_uri <- paste0('s3://', token$bucket, '/', token$baseKey)
} else {
  base_s3_uri <- paste0('s3://', PARQUET_BUCKET, '/', PARQUET_BUCKET_BASE_KEY)
}

base_s3_uri_archive <- paste0('s3://', PARQUET_BUCKET_EXTERNAL, '/', PARQUET_BUCKET_BASE_KEY_ARCHIVE)


# Configure the environment with AWS token --------------------------------
Sys.setenv('AWS_ACCESS_KEY_ID'=token$accessKeyId,
           'AWS_SECRET_ACCESS_KEY'=token$secretAccessKey,
           'AWS_SESSION_TOKEN'=token$sessionToken)

s3 <- arrow::S3FileSystem$create(access_key = token$accessKeyId,
                                 secret_key = token$secretAccessKey,
                                 session_token = token$sessionToken,
                                 region="us-east-1")

archive_uri <- paste0(token$bucket, '/', token$baseKey, '/archive/')
archive_list <- s3$GetFileInfo(arrow::FileSelector$create(archive_uri, recursive=F))
latest_archive <- archive_list[[length(archive_list)]]$path


# Sync bucket to local dir ------------------------------------------------
unlink(AWS_PARQUET_DOWNLOAD_LOCATION, recursive = T, force = T)
sync_cmd <- glue::glue('aws s3 sync s3://{latest_archive}/ {AWS_PARQUET_DOWNLOAD_LOCATION} --exclude "*owner.txt*" --exclude "*archive*"')
system(sync_cmd)


# Remove withdrawn participants -------------------------------------------
source("~/recover-parquet-external/scripts/withdrawal/remove_withdrawn_participants.R")


# Filter parquet datasets -------------------------------------------------
source('~/recover-parquet-external/scripts/filtering/filtering.R')

# Copy unfiltered parquet datasets to new location with filtered parquet datasets
unlink(PARQUET_FINAL_LOCATION, recursive = T, force = T)
duplicate_folder(source_folder = PARQUET_FILTERED_LOCATION, 
                 destination_folder = PARQUET_FINAL_LOCATION)

copy_folders_reparent(AWS_PARQUET_DOWNLOAD_LOCATION, PARQUET_FINAL_LOCATION)

# Remove intermediate folders
unlink(PARQUET_FILTERED_LOCATION, recursive = T, force = T)


# De-identify parquet datasets --------------------------------------------
source('~/recover-parquet-external/scripts/deidentification/deidentification.R')


# Sync final parquets to bucket -------------------------------------------
date <- 
  latest_archive %>% 
  stringr::str_extract("[0-9]{4}_[0-9]{2}_[0-9]{2}") %>% 
  stringr::str_replace_all(pattern = "_", replacement = "-")

sync_cmd <- glue::glue('aws s3 --profile service-catalog sync {PARQUET_FINAL_LOCATION} {base_s3_uri_archive}{date}/ --exclude "*owner.txt*" --exclude "*archive*"')
system(sync_cmd)

# Sync new date dir in staging bucket to local
unlink(AWS_PARQUET_DOWNLOAD_LOCATION, recursive = T, force = T)
unlink(AWS_ARCHIVE_DOWNLOAD_LOCATION, recursive = T, force = T)
sync_cmd <- glue::glue('aws s3 --profile service-catalog sync {base_s3_uri_archive}{date}/ {AWS_ARCHIVE_DOWNLOAD_LOCATION}/{date}/ --exclude "*owner.txt*" --exclude "*archive*"')
system(sync_cmd)

# Modify cohort identifier in dir name
junk <- sapply(list.dirs(AWS_ARCHIVE_DOWNLOAD_LOCATION), replace_equal_with_underscore)

# Generate manifest of existing files
SYNAPSE_AUTH_TOKEN <- Sys.getenv('SYNAPSE_AUTH_TOKEN')
manifest_cmd <- glue::glue('SYNAPSE_AUTH_TOKEN="{SYNAPSE_AUTH_TOKEN}" synapse manifest --parent-id {PARQUET_FOLDER_ARCHIVE} --manifest ./current_manifest.tsv {AWS_ARCHIVE_DOWNLOAD_LOCATION}')
system(manifest_cmd)


# Index files in Synapse --------------------------------------------------
# Get a list of all files to upload and their synapse locations (parentId)
STR_LEN_PARQUET_FINAL_LOCATION <- stringr::str_length(AWS_ARCHIVE_DOWNLOAD_LOCATION)

## List all local files present (from manifest)
synapse_manifest <- 
  read.csv('./current_manifest.tsv', sep = '\t', stringsAsFactors = F) %>%
  dplyr::filter(!grepl('owner.txt', path)) %>%
  dplyr::rowwise() %>%
  dplyr::mutate(file_key = stringr::str_sub(string = path, start = STR_LEN_PARQUET_FINAL_LOCATION+2)) %>%
  dplyr::mutate(s3_file_key = paste0(PARQUET_BUCKET_BASE_KEY_ARCHIVE, file_key)) %>%
  dplyr::mutate(md5_hash = as.character(tools::md5sum(path))) %>%
  dplyr::ungroup() %>% 
  dplyr::mutate(file_key = gsub("cohort_", "cohort=", file_key),
                s3_file_key = gsub("cohort_", "cohort=", s3_file_key))

# List all files currently indexed in Synapse
synapse_fileview <- 
  synapser::synTableQuery(paste0('SELECT * FROM ', SYNAPSE_FILEVIEW_ID))$filepath %>%
  read.csv()
synapse_fileview <- 
  synapser::synTableQuery(paste0('SELECT * FROM ', SYNAPSE_FILEVIEW_ID))$filepath %>% 
  read.csv()

# Find the files in the manifest that are not yet indexed in Synapse
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

# Index each file in Synapse
latest_commit <- gh::gh("/repos/:owner/:repo/commits/main", owner = "Sage-Bionetworks", repo = "recover-parquet-external")
latest_commit_this_file <- paste0(latest_commit$html_url %>% stringr::str_replace("commit", "blob"), "/scripts/main/sts_synindex_external.R")

act <- synapser::Activity(name = "Indexing",
                          description = "Indexing external parquet datasets",
                          used = paste0("s3://", latest_archive), 
                          executed = latest_commit_this_file)

if(nrow(synapse_manifest_to_upload) > 0){
  for(file_number in seq_len(nrow(synapse_manifest_to_upload))){
    tmp <- synapse_manifest_to_upload[file_number, c("path", "parent", "s3_file_key")]
    
    absolute_file_path <- tools::file_path_as_absolute(tmp$path)
    
    temp_syn_obj <- 
      synapser::synCreateExternalS3FileHandle(
        bucket_name = PARQUET_BUCKET_EXTERNAL,
        s3_file_key = tmp$s3_file_key,
        file_path = absolute_file_path,
        parent = tmp$parent)
    
    new_fileName <- stringr::str_replace_all(temp_syn_obj$fileName, ':', '_colon_')
    
    f <- File(dataFileHandleId = temp_syn_obj$id,
              parentId = tmp$parent,
              name = new_fileName)
    
    f <- synStore(f,  activity = act)
    
  }
}
