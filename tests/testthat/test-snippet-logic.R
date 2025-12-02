library(testthat)

## Source the logic file that contains the functions to be tested.
## The path is relative to the `tests/testthat/` directory.

DIR_CODE <- "~/Dropbox/proj/litanai/"

source(paste0(DIR_CODE, "shiny/logic.R"))

# --- Test Setup ---
# Establish a connection to the database for the tests.
# This code runs once when the test file is executed.
con <- DBI::dbConnect(RClickhouse::clickhouse(), dbname = "litanai")

# Define some dummy data that mimics the inputs from the Shiny app.
EXAMPLE_DOC_IDS <- c("FilippiMazzola_Wit_2024_neural.pdf", "Bianchi_etal_2024_rem.pdf")

# --- Test Cases ---

context("Snippet Generation Logic: gd_snippets_from_db")

test_that("basic search query works", {

    search_term <- "regression,likelihood"

    l_searchterms_cbn <- gl_searchterms_cbn(search_term)
    expect_true(len(l_searchterms_cbn) > 0)
    qry_fmt <- gc_qry_fmt(l_searchterms_cbn)
    expect_true(nchar(qry_fmt) > 10)
    dt_res <- gd_res(qry_fmt)
    expect_true(nrow(dt_res) > 5)
})


test_that("Snippet search with 2 terms returns results", {

    search_term <- "regression,likelihood"
      ## Execute the function
    dt_snippets <- gd_snippets_from_db(con, EXAMPLE_DOC_IDS, search_term, len_pre = 200, len_post = 200)
    dt_snippets[, snippet]
  
    ## Add expectations about the output
    ## 1. We expect to get a data.table back
    expect_s3_class(dt_snippets, "data.table")
  
    ## 2. We expect to get more than zero snippets
    expect_gt(nrow(dt_snippets), 0)
  
    ## 3. We expect every returned snippet to contain both words (case-insensitive)
    expect_true(all(grepl("regression", dt_snippets$snippet, ignore.case = TRUE)))
    expect_true(all(grepl("likelihood", dt_snippets$snippet, ignore.case = TRUE)))
})

## test_that("Snippet search with 3 terms returns results", {
##   # This was the failing test case from our previous discussion
##   search_term <- "regression,likelihood,relational"
  
##   dt_snippets <- gd_snippets_from_db(con, EXAMPLE_DOC_IDS, search_term, len_pre = 200, len_post = 200)
  
##   # Expectations
##   expect_s3_class(dt_snippets, "data.table")
##   expect_gt(nrow(dt_snippets), 0)
##   expect_true(all(grepl("regression", dt_snippets$snippet, ignore.case = TRUE)))
##   expect_true(all(grepl("likelihood", dt_snippets$snippet, ignore.case = TRUE)))
##   expect_true(all(grepl("relational", dt_snippets$snippet, ignore.case = TRUE)))
## })

test_that("Source documents contain all 3 terms", {
  # This test isolates whether the underlying data assumption is correct.
  # If this fails, it means the documents don't contain all terms, which is why the snippet search fails.
  query <- glue::glue_sql(
    "SELECT count() FROM litanai.littext WHERE key IN ({EXAMPLE_DOC_IDS*})
     AND (text ILIKE '%regression%' AND text ILIKE '%likelihood%' AND text ILIKE '%relational%')",
    .con = con
  )
  
  doc_count <- dbGetQuery(con, query)[[1]]
  
  # Expectation: At least one of the documents should contain all three terms.
  expect_gt(doc_count, 0, label = "Number of documents containing all 3 terms")
})

test_that("Snippet search with non-existent term returns zero results", {
  search_term <- "regression,nonexistenttermxyz"
  
  dt_snippets <- gd_snippets_from_db(con, EXAMPLE_DOC_IDS, search_term, len_pre = 200, len_post = 200)
  
  # We expect to get zero rows back
  expect_equal(nrow(dt_snippets), 0)
})

# --- Test Teardown ---
# It's good practice to disconnect from the database when tests are done.
# testthat runs this automatically at the end of the file.
dbDisconnect(con)
