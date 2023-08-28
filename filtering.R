# Calculate age from DoB -----------------------------------------------------
dob2age <- function(dataset, column, output=PARQUET_FILTERED_LOCATION) {
  if (dataset %in% list.dirs(AWS_PARQUET_DOWNLOAD_LOCATION, full.names = F)) {
    input_path <- paste0(AWS_PARQUET_DOWNLOAD_LOCATION, '/', dataset)
    final_path <- paste0(output, '/', dataset, '/')
    
    arrow::open_dataset(sources = input_path) %>% 
      dplyr::mutate(age = lubridate::year(lubridate::today())-lubridate::year(lubridate::as_date(!!sym(column)))) %>% 
      arrow::write_dataset(path = input_path, max_rows_per_file = 900000)
  }
}

dob2age("dataset_enrolledparticipants", "DateOfBirth")

# Drop columns with potentially identifying info --------------------------
drop_cols_datasets <- function(dataset, columns=c(), output=PARQUET_FILTERED_LOCATION) {
  if (dataset %in% list.dirs(AWS_PARQUET_DOWNLOAD_LOCATION, full.names = F)) {
    input_path <- paste0(AWS_PARQUET_DOWNLOAD_LOCATION, '/', dataset)
    final_path <- paste0(output, '/', dataset, '/')
    
    arrow::open_dataset(sources = input_path) %>% 
      dplyr::select(!columns) %>% 
      arrow::write_dataset(path = final_path, max_rows_per_file = 900000)
  }
}

lapply(seq_along(datasets_to_filter), function(i) {
  cat("Dropping ", cols_to_drop[[i]], " from ", datasets_to_filter[i], "\n")
  drop_cols_datasets(dataset = datasets_to_filter[i], columns = cols_to_drop[[i]])
})
