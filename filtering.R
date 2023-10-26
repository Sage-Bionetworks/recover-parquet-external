# Calculate age from DoB -----------------------------------------------------
dob2age <- function(dataset, column, output=PARQUET_FILTERED_LOCATION) {
  if (dataset %in% list.dirs(AWS_PARQUET_DOWNLOAD_LOCATION, full.names = F)) {
    input_path <- paste0(AWS_PARQUET_DOWNLOAD_LOCATION, '/', dataset)
    
    arrow::open_dataset(sources = input_path) %>% 
      dplyr::mutate(age = lubridate::year(lubridate::today())-lubridate::year(lubridate::as_date(!!sym(column)))) %>% 
      arrow::write_dataset(path = input_path, 
                           max_rows_per_file = 100000, 
                           partitioning = c('cohort'), 
                           existing_data_behavior = 'delete_matching')
  }
}

dob2age("dataset_enrolledparticipants", "DateOfBirth")

# Drop columns with potentially identifying info --------------------------
drop_cols_datasets <- function(dataset, columns=c(), input = AWS_PARQUET_DOWNLOAD_LOCATION, output=PARQUET_FILTERED_LOCATION) {
  if (dataset %in% list.dirs(input, full.names = F)) {
    input_path <- paste0(input, '/', dataset)
    final_path <- paste0(output, '/', dataset, '/')
    
    arrow::open_dataset(sources = input_path) %>% 
      dplyr::select(!columns) %>% 
      arrow::write_dataset(path = final_path, 
                           max_rows_per_file = 100000,
                           partitioning = c('cohort'), 
                           existing_data_behavior = 'delete_matching')
  }
}

unlink(PARQUET_FILTERED_LOCATION, recursive = T, force = T)

synLogin()

pii_to_drop <- synGet('syn52523394')$path %>% read.csv()

lapply(seq_len(nrow(pii_to_drop)), function(i) {
  cat("Dropping", pii_to_drop$column_to_be_dropped[[i]], "from", pii_to_drop$dataset[[i]], "\n")
  drop_cols_datasets(dataset = pii_to_drop$dataset[[i]], columns = pii_to_drop$column_to_be_dropped[[i]])
})

rm(pii_to_drop)
