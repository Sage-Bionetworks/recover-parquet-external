rm(list = names(config::get(config = "staging")))
config::get(config = "prod") %>% list2env(envir = .GlobalEnv)

validated_date <- readline("Enter name of validated staging folder in yyyy-mm-dd format: ")
cmd <- glue::glue("aws s3 --profile service-catalog cp {base_s3_uri_staging}/{validated_date} {base_s3_uri_archive}")
rm(validated_date)
