# Datasets that contain ParticipantIdentifier column
contains_pid <- 
  sapply(list.dirs(AWS_PARQUET_DOWNLOAD_LOCATION, recursive = F), function(x) {
    grepl("ParticipantIdentifier", open_dataset(x)$metadata$org.apache.spark.sql.parquet.row.metadata)
  }) %>% 
  tibble::enframe()

contains_pid_false <- contains_pid$name[which(contains_pid$value==FALSE)]

sapply(contains_pid_false, function(x) {
  open_dataset(x)
})

# fitbit_sleeplogs -> fitbit_sleeplogdetails: match on LogId
# symptomlog -> symptomlog_value_symptoms, symptomlog_value_treatments: match on DataPointKey
# healthkitv2electrocardiogram -> healthkitv2electrocardiogram_subsamples: match on HealthKitECGSampleKey
# healthkitv2heartbeat -> healthkitv2heartbeat_subsamples: match on HealthKitHeartbeatSampleKey
# healthkitv2workouts -> healthkitv2workouts_events: match on HealthKitWorkoutKey


participants_to_withdraw <- 
  arrow::open_dataset(paste0(AWS_PARQUET_DOWNLOAD_LOCATION, "/dataset_enrolledparticipants/")) %>% 
  dplyr::select(ParticipantIdentifier, CustomFields_EOPRemoveData) %>% 
  dplyr::filter(CustomFields_EOPRemoveData==1) %>%
  dplyr::pull(ParticipantIdentifier, as_vector = TRUE) %>% 
  unique()

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
