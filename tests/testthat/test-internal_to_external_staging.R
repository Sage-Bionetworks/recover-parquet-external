library(testthat)

# Test for duplicate_folder
test_that("duplicate_folder correctly duplicates a folder", {
  td <- "tmpdir"
  dir.create(td)
  source_folder <- file.path(td, "source_folder")
  destination_folder <- file.path(td, "destination_folder")
  dir.create(source_folder)
  file.create(file.path(source_folder, "file1.txt"))
  
  # Create a separate temporary directory for the destination
  expect_true(dir.exists(source_folder))
  result <- duplicate_folder(source_folder, destination_folder)
  
  expect_true(file.exists(file.path(destination_folder, "file1.txt")))
  expect_equal(result, destination_folder)
  
  # Use expect_message to capture the warning
  # expect_message(result, "Destination folder already exists. Files might be overwritten.")
  
  # Clean up the temporary directories
  unlink(td, recursive = T)
})

# Test for copy_folders_reparent
test_that("copy_folders_reparent correctly copies folders", {
  td <- "tmpdir"
  dir.create(td)
  source_folder <- file.path(td, "source_folder")
  destination_folder <- file.path(td, "destination_folder")
  dir.create(source_folder)
  dir.create(destination_folder)
  dir.create(file.path(source_folder, "folder1"))
  file.create(file.path(source_folder, "folder1", "folder1.txt"))
  dir.create(file.path(source_folder, "folder2"))
  file.create(file.path(source_folder, "folder2", "folder2.txt"))
  result <- copy_folders_reparent(source_folder, destination_folder)
  expect_true(dir.exists(file.path(destination_folder, "folder1")))
  expect_true(dir.exists(file.path(destination_folder, "folder2")))
  expect_true(file.exists(file.path(destination_folder, "folder1", "folder1.txt")))
  expect_true(file.exists(file.path(destination_folder, "folder2", "folder2.txt")))
  # expect_output(result, "Copied.*")
  unlink(td, recursive = T)
})

# Test for replace_equal_with_underscore
test_that("replace_equal_with_underscore correctly replaces equal sign with underscore", {
  td <- "tmpdir"
  dir.create(td)
  original_path <- "path=with=equals"
  dir.create(file.path(td, original_path))
  replace_equal_with_underscore(file.path(td, original_path))
  expect_equal(list.dirs(td, recursive = F, full.names = F), "path_with_equals")
  expect_true(dir.exists(list.dirs(td, recursive = F, full.names = T)))
  # expect_output(new_path, "Renamed.*")
  unlink(td, recursive = T)
})
