library(synapser)
library(synapserutils)
library(tidyr)
library(dplyr)

synLogin()

files <- synapserutils::syncFromSynapse('syn52316269')

names(files) <- 
  lapply(files, function(x) x$name) %>% 
  {gsub("Dictionary_|.csv", "", ., perl = T)}

store_dicts <- function(files_list) {
  dicts <- list()
  # dicts_approved <- list()
  # dicts_unapproved <- list()
  
  for (file_entry in files_list) {
    file_path <- file_entry$path
    filename <- tools::file_path_sans_ext(basename(file_path))
    new_dict_name <- sub("^Dictionary_", "", filename)
    data <- read.csv(file_path)
    # approved <- data[[1]][which(data[[2]]=="APPROVED")]
    # unapproved <- data[[1]][which(data[[2]]=="UNAPPROVED")]
    # dicts_approved[[new_dict_name]] <- approved
    # dicts_unapproved[[new_dict_name]] <- unapproved
    dicts[[new_dict_name]] <- data
  }
  return(dicts)
  # return(list(dicts_approved, dicts_unapproved))
}

dicts <- store_dicts(files_list = files)
# names(dicts) <- c('approved', 'unapproved')

# # left_join(dicts[[1]], open_dataset(paste0('./temp_aws_parquet/',names(dicts)[1])) %>% collect())
# open_dataset(paste0('./temp_aws_parquet/', names(dicts)[1])) %>% 
#   collect() %>% 
#   mutate(colnames(dicts[[1]])[1]=if_else(tolower(colnames(dicts[[1]])[1]) %in% dicts[[1]][[1]]))
# 
# for (i in seq_along(dicts)) {
#   col_name <- as.character(colnames(dicts[[i]])[1])
#   status_col <- as.character(colnames(dicts[[i]])[2])
# 
#   parquet_dataset <- file.path(AWS_PARQUET_DOWNLOAD_LOCATION, paste0(names(dicts)[i]))
# 
#   if (dir.exists(parquet_dataset)) {
#     parquet_table <- arrow::open_dataset(parquet_dataset) %>% collect()
# 
#     search_values <- tolower(as.character(dicts[[i]][[1]]))
#     matching_rows <- tolower(parquet_table[[col_name]]) %in% search_values
# 
#     update_indices <- which(!is.na(matching_rows))
#     non_approved_indices <- update_indices[parquet_table[[status_col]][matching_rows[update_indices]] != "APPROVED"]
# # 
# #     if (length(non_approved_indices) > 0) {
# #       parquet_table[[col_name]][matching_rows[non_approved_indices]] <- NA
# #       arrow::write_parquet(parquet_table, parquet_dataset)
# #     }
#   }
# }
# 
# fitbitactivitylogs <- 
#   open_dataset(paste0('./temp_aws_parquet/', names(dicts)[3])) %>% 
#   collect() %>% 
#   mutate(ActivityName = tolower(ActivityName))
# 
# left_join(fitbitactivitylogs, dicts[[3]]) %>% select(ActivityName, status) %>% 
#   mutate(ActivityName=ifelse(status!="APPROVED", NA, ActivityName)) %>% View

fx <- function(dicts_list, parquet_dir) {
  out_list <- list()
  for (i in seq_along(dicts_list)) {
    var_name <- colnames(dicts_list[[i]])[1]
    status_col <- colnames(dicts_list[[i]])[2]
    
    df <- open_dataset(paste0(parquet_dir, '/', names(dicts_list)[i])) %>% collect()
    df[[var_name]] <- tolower(df[[var_name]])
    
    out <- left_join(df, dicts_list[[i]])
    out[[var_name]] <- trimws(out[[var_name]])
    # out[[var_name]] <- ifelse(out[[status_col]]!="APPROVED", NA, out[[var_name]])
    
    out_list[[i]] <- out
  }
  return(out_list)
}

tmp <- fx(dicts, AWS_PARQUET_DOWNLOAD_LOCATION)

