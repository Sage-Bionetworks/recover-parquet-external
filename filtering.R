# Functions ---------------------------------------------------------------
#' Calculate age from Date of Birth
#'
#' This function calculates the age of individuals based on their date of birth.
#'
#' @param dataset The name of the dataset to process.
#' @param column The name of the column in the dataset that contains Date of Birth (DoB) information.
#' @param input The location where the Parquet dataset is stored. Default is AWS_PARQUET_DOWNLOAD_LOCATION.
#'
#' @return None (invisibly returns the filtered dataset)
#'
#' @examples
#' dob2age("my_dataset", "date_of_birth_column")
#' 
dob2age <- function(dataset, column, input = AWS_PARQUET_DOWNLOAD_LOCATION, partitions = NULL) {
  if (dataset %in% list.dirs(input, full.names = F)) {
    input_path <- paste0(input, '/', dataset)
    
    arrow::open_dataset(sources = input_path) %>% 
      dplyr::mutate(age = lubridate::year(lubridate::today())-lubridate::year(lubridate::as_date(!!sym(column)))) %>% 
      arrow::write_dataset(path = input_path, 
                           max_rows_per_file = 100000, 
                           partitioning = partitions, 
                           existing_data_behavior = 'delete_matching')
  }
}

#' Drop columns with potentially identifying information
#'
#' This function removes specified columns from a dataset to eliminate potentially identifying information.
#'
#' @param dataset The name of the dataset to process.
#' @param columns A character vector of column names to be dropped from the dataset.
#' @param input The location where the Parquet dataset is stored. Default is AWS_PARQUET_DOWNLOAD_LOCATION.
#' @param output The location where the filtered Parquet dataset will be saved. Default is PARQUET_FILTERED_LOCATION.
#'
#' @return None (invisibly returns the filtered dataset)
#'
#' @examples
#' drop_cols_datasets("my_dataset", c("column1", "column2"), input = "./temp1", output = "./temp2")
#'
# Drop columns with potentially identifying info
drop_cols_datasets <- function(dataset, columns=c(), input = AWS_PARQUET_DOWNLOAD_LOCATION, output=PARQUET_FILTERED_LOCATION, partitions = NULL) {
  if (dataset %in% list.dirs(input, full.names = F)) {
    input_path <- paste0(input, '/', dataset)
    final_path <- paste0(output, '/', dataset, '/')
    
    arrow::open_dataset(sources = input_path) %>% 
      dplyr::select(!dplyr::any_of(columns)) %>% 
      arrow::write_dataset(path = final_path, 
                           max_rows_per_file = 100000,
                           partitioning = partitions, 
                           existing_data_behavior = 'delete_matching',
                           basename_template = paste0("part-0000{i}.", as.character("parquet")))
  }
}


# Filtering ---------------------------------------------------------------
dob2age(dataset = "dataset_enrolledparticipants", 
        column = "DateOfBirth", 
        input = AWS_PARQUET_DOWNLOAD_LOCATION, 
        partitions = "cohort")

unlink(PARQUET_FILTERED_LOCATION, recursive = T, force = T)

synLogin()

pii_to_drop <- synGet('syn52523394')$path %>% read.csv()

datasets_to_filter <- pii_to_drop$dataset %>% unique()
cols_to_drop <- lapply(datasets_to_filter, function(x) {
  pii_to_drop$column_to_be_dropped[which(pii_to_drop$dataset==x)]
  })

tmp <- 
  lapply(seq_len(nrow(datasets_to_filter)), function(i) {
    cat(i, "Dropping", cols_to_drop[[i]], "from", datasets_to_filter[[i]], "\n")
    drop_cols_datasets(dataset = datasets_to_filter[[i]], 
                       columns = cols_to_drop[[i]], 
                       input = AWS_PARQUET_DOWNLOAD_LOCATION, 
                       output = PARQUET_FILTERED_LOCATION, 
                       partitions = "cohort")
    })

