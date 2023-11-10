library(synapser)

synapser::synLogin(authToken = Sys.getenv('SYNAPSE_AUTH_TOKEN'))

base_s3_uri_staging <- paste0('s3://', PARQUET_BUCKET_EXTERNAL, '/', PARQUET_BUCKET_BASE_KEY_ARCHIVE)

rm(list = names(config::get(config = "staging")))
config::get(config = "prod") %>% list2env(envir = .GlobalEnv)

base_s3_uri_archive <- paste0('s3://', PARQUET_BUCKET_EXTERNAL, '/', PARQUET_BUCKET_BASE_KEY_ARCHIVE)

validated_date <- readline("Enter name of validated staging folder in yyyy-mm-dd format: ")
cmd <- glue::glue("aws s3 --profile service-catalog cp {base_s3_uri_staging}{validated_date} {base_s3_uri_archive} --exclude '*owner.txt*' --exclude '*archive*'")
rm(validated_date)

# Sync entire bucket to local
unlink(AWS_PARQUET_DOWNLOAD_LOCATION, recursive = T, force = T)
unlink(AWS_ARCHIVE_DOWNLOAD_LOCATION, recursive = T, force = T)
sync_cmd <- glue::glue('aws s3 --profile service-catalog sync {base_s3_uri_archive} {AWS_ARCHIVE_DOWNLOAD_LOCATION} --exclude "*owner.txt*" --exclude "*archive*"')
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
latest_commit_tree_url <- latest_commit$html_url %>% stringr::str_replace("commit", "tree")

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
    
    f <- synStore(f, 
                  activityName = "Indexing", 
                  activityDescription = "Indexing external parquet datasets",
                  used = PARQUET_FOLDER_INTERNAL, 
                  executed = latest_commit_tree_url)
    
  }
}
