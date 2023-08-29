library(synapser)
library(synapserutils)
library(tidyr)
library(dplyr)

synLogin()

files <- synapserutils::syncFromSynapse('syn52316269')
# files <- synGetChildren('syn52316269')$asList()

names(files) <- 
  lapply(files, function(x) x$name) %>% 
  {gsub("Dictionary_|.csv", "", ., perl = T)}

for (file_entry in files) {
  file_path <- file_entry$path
  filename <- tools::file_path_sans_ext(basename(file_path))
  new_dict_name <- sub("^Dictionary_dataset", "dict", filename)
  data <- read.csv(file_path)
  assign(new_dict_name, data, envir = .GlobalEnv)
}
