library(testthat)

test_that("dob2age correctly calculates age from Date of Birth", {
  test_dataset <- data.frame(
    date_of_birth = as.Date(c("1990-01-15", "1985-05-10", "2000-12-30"))
  )
  
  arrow::write_dataset(test_dataset, path = "test_dob2age")
  
  dob2age("test_dob2age", "date_of_birth", input = ".", partitions = NULL)
  
  modified_dataset <- arrow::open_dataset("test_dob2age")
  
  expect_true("age" %in% names(modified_dataset))
  expect_equal((modified_dataset %>% collect %>% pull(age)), c(33, 38, 23))
  
  unlink("test_dob2age/part-0.parquet")
})

test_that("drop_cols_datasets correctly drops specified columns", {
  test_dataset <- data.frame(
    column1 = c(1, 2, 3),
    column2 = c("A", "B", "C"),
    column3 = c(0.1, 0.2, 0.3)
  )
  
  arrow::write_dataset(test_dataset, path = "test_drop_cols")
  
  columns_to_drop <- c("column1", "column2")
  
  drop_cols_datasets("test_drop_cols", columns = columns_to_drop, input = ".", output = ".")
  
  modified_dataset <- arrow::open_dataset("test_drop_cols")
  
  expect_true(all(!names(modified_dataset) %in% columns_to_drop))
  
  unlink("test_drop_cols/part-0.parquet")
})
