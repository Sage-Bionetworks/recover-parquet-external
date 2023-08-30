library(synapser)
library(synapserutils)
library(tidyr)
library(dplyr)

synLogin()

system('synapse get -r syn52316269 --downloadLocation ./dictionaries/ --manifest suppress')

store_dicts <- function(files_dir) {
  dicts <- list()

  for (f in list.files(files_dir)) {
    file_path <- file.path(files_dir, file.path(f))
    filename <- tools::file_path_sans_ext(basename(file_path))
    new_dict_name <- sub("^Dictionary_", "", filename)
    data <- read.csv(file_path)
    dicts[[new_dict_name]] <- data
  }
  return(dicts)
}

dicts <- store_dicts('./dictionaries')

for (i in seq_along(dicts)) {
  dicts[[i]][[1]] <- trimws(dicts[[i]][[1]])
}

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
# tmpdf <-
#   open_dataset(paste0('./temp_aws_parquet/', names(dicts)[3])) %>%
#   collect()
# 
# # left_join(tmpdf, dicts[[3]]) %>% select(ActivityName, status) %>%
# #   mutate(ActivityName=ifelse(status!="APPROVED", NA, ActivityName)) %>% View
# 
# var_name <- colnames(dicts[[1]])[1]
# status_col <- colnames(dicts[[1]])[2]
# 
# tmpdf[[var_name]] <- tolower(tmpdf[[var_name]])
# 
# out <- left_join(tmpdf, dicts[[3]])
# out[[var_name]] <- trimws(out[[var_name]])
# new_to_review <- out[[var_name]][which(is.na(out[[status_col]]))] %>% unique()
# out[[var_name]] <- ifelse(out[[status_col]]=="APPROVED", out[[var_name]], NA)

deidentify <- function(dicts_list, parquet_dir) {
  out_list <- list()
  review_list <- list()
  
  for (i in seq_along(dicts_list)) {
    var_name <- colnames(dicts_list[[i]])[1]
    status_col <- colnames(dicts_list[[i]])[2]
    
    df <- open_dataset(paste0(parquet_dir, '/', names(dicts_list)[i])) %>% collect()
    df[[var_name]] <- tolower(df[[var_name]])
    
    out <- df
    out[[var_name]] <- trimws(out[[var_name]])

    needs_review <- character(0)
    
    for (j in 1:nrow(out)) {
      val <- out[[var_name]][j]
      status <- dicts_list[[i]][[2]][which(dicts_list[[i]][[1]]==val)]
      
      if (val %in% dicts_list[[i]][[var_name]]) {
        if (status == "UNAPPROVED") {
          out[[var_name]][j] <- NA
        }
      } else {
        needs_review <- c(needs_review, val)
        out[[var_name]][j] <- NA
      }
    }
    
    needs_review <- unique(needs_review)
    
    out_list[[i]] <- out
    review_list[[i]] <- needs_review
  }
  names(out_list) <- names(dicts_list)
  names(review_list) <- names(dicts_list)
  
  results <- list(out_list, review_list)
  names(results) <- c('deidentified_datasets', 'values_to_review')
  
  return(results)
}

deidentified_results <- deidentify(dicts, AWS_PARQUET_DOWNLOAD_LOCATION)

for (i in seq_along(deidentified_results$deidentified_datasets)) {
  dir <- file.path(AWS_PARQUET_DOWNLOAD_LOCATION, names(deidentified_results$deidentified_datasets)[[i]])
  unlink(dir, recursive = T, force = T)
  dir.create(dir)
  
  arrow::write_dataset(dataset = deidentified_results$deidentified_datasets[[i]], 
                       path = file.path(AWS_PARQUET_DOWNLOAD_LOCATION, names(deidentified_results$deidentified_datasets)[[i]]), 
                       max_rows_per_file = 900000)
}
