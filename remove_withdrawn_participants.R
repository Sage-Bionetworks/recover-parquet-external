
# Functions ---------------------------------------------------------------

#' Get Identifier Variable Values for Withdrawn Participants
#'
#' This function gets the values of an identifier variable in 
#' datasets that do not contain the ParticipantIdentifier variable, but 
#' only for the corresponding ParticipantIdentifer values that are found 
#' in a list containing ParticipantIdentifier values of withdrawn participants.
#'
#' @param dataset_name The name of a dataset that has the ParticipantIdentifier 
#' variable and the mapping identifier variable (`mappingID_var`).
#' @param mappingID_var The name of an identifier variable that can be 
#' mapped to ParticipantIdentifier in the `dataset_name`'s parent dataset.
#'
#' @return The values of `mappingID_var` after filtering the dataset.
#'
#' @examples
#' values_to_withdraw <- 
#' get_mappingID_vals_to_withdraw("my_dataset", "DataPointKey")
#' 
get_mappingID_vals_to_withdraw <- function(dataset_name, mappingID_var) {
  arrow::open_dataset(paste0(AWS_PARQUET_DOWNLOAD_LOCATION, "/", dataset_name, "/")) %>% 
    dplyr::select(dplyr::all_of(c("ParticipantIdentifier", mappingID_var))) %>% 
    dplyr::filter(ParticipantIdentifier %in% participants_to_withdraw) %>% 
    dplyr::collect() %>% 
    dplyr::pull(mappingID_var)
}


# Main --------------------------------------------------------------------

participants_to_withdraw <- 
  arrow::open_dataset(paste0(AWS_PARQUET_DOWNLOAD_LOCATION, "/dataset_enrolledparticipants/")) %>% 
  dplyr::select(ParticipantIdentifier, CustomFields_EOPRemoveData) %>% 
  dplyr::filter(CustomFields_EOPRemoveData==1) %>%
  dplyr::collect() %>% 
  dplyr::pull(ParticipantIdentifier) %>% 
  unique()

# Datasets that do not contain ParticipantIdentifier column
contains_pid_false <- 
  sapply(list.dirs(AWS_PARQUET_DOWNLOAD_LOCATION, recursive = F), function(x) {
    grepl("ParticipantIdentifier", open_dataset(x)$metadata$org.apache.spark.sql.parquet.row.metadata)
  }) %>% 
  tibble::enframe() %>% 
  dplyr::filter(value==FALSE) %>% 
  dplyr::pull(name)

# fitbitsleeplogs -> fitbitsleeplogs_sleeplogdetails: match on LogId
# symptomlog -> symptomlog_value_symptoms, symptomlog_value_treatments: match on DataPointKey
# healthkitv2electrocardiogram -> healthkitv2electrocardiogram_subsamples: match on HealthKitECGSampleKey
# healthkitv2heartbeat -> healthkitv2heartbeat_subsamples: match on HealthKitHeartbeatSampleKey
# healthkitv2workouts -> healthkitv2workouts_events: match on HealthKitWorkoutKey

fitbitsleeplogs_sleeplogdetails_to_withdraw <-
  get_mappingID_vals_to_withdraw("dataset_fitbitsleeplogs", "LogId")

symptomlog_value_symptoms_to_withdraw <-
  get_mappingID_vals_to_withdraw("dataset_symptomlog", "DataPointKey")

symptomlog_value_treatments_to_withdraw <-
  symptomlog_value_symptoms_to_withdraw

healthkitv2electrocardiogram_subsamples_to_withdraw <-
  get_mappingID_vals_to_withdraw("dataset_healthkitv2electrocardiogram", "HealthKitECGSampleKey")

healthkitv2heartbeat_subsamples_to_withdraw <-
  get_mappingID_vals_to_withdraw("dataset_healthkitv2heartbeat", "HealthKitHeartbeatSampleKey")

healthkitv2workouts_events_to_withdraw <-
  get_mappingID_vals_to_withdraw("dataset_healthkitv2workouts", "HealthKitWorkoutKey")

lapply(list.dirs(AWS_PARQUET_DOWNLOAD_LOCATION, recursive = F), function(x) {
  if (x %in% contains_pid_false) {
    # use mapping above to filter out data
  } else {
    d <-
      arrow::open_dataset(x) %>%
      filter(!ParticipantIdentifier %in% participants_to_withdraw) %>%
      arrow::write_dataset(path = x,
                           max_rows_per_file = 100000,
                           partitioning = "cohort",
                           existing_data_behavior = 'delete_matching',
                           basename_template = paste0("part-0000{i}.", as.character("parquet")))
  }
})

# lapply(list.dirs("test_dir", recursive = F), function(x) {
#   grepl("RA12301-00099", (open_dataset(x) %>% select(ParticipantIdentifier) %>% collect() %>% as.list()))
# })
# 
# lapply(list.dirs("test_dir_new", recursive = F), function(x) {
#   grepl("RA12301-00099", (open_dataset(x) %>% select(ParticipantIdentifier) %>% collect() %>% as.list()))
# })
