base_s3_uri_staging <- paste0('s3://', PARQUET_BUCKET_EXTERNAL, '/', PARQUET_BUCKET_BASE_KEY_ARCHIVE)

rm(list = names(config::get(config = "staging")))
config::get(config = "prod") %>% list2env(envir = .GlobalEnv)

base_s3_uri_archive <- paste0('s3://', PARQUET_BUCKET_EXTERNAL, '/', PARQUET_BUCKET_BASE_KEY_ARCHIVE)

validated_date <- readline("Enter name of validated staging folder in yyyy-mm-dd format: ")
cmd <- glue::glue("aws s3 --profile service-catalog cp {base_s3_uri_staging}{validated_date} {base_s3_uri_archive}")
rm(validated_date)

