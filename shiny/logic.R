# This file contains the core data processing and business logic for the Shiny app.
# It is sourced by server.R and can be sourced by test files.

library(data.table)
library(RClickhouse)
library(dbplyr)
library(purrr)
library(DBI)

# --- Database Connection ---
# This function is defined here to be accessible by test scripts as well.
check_connection <- function(con) {
  if (!dbIsValid(con)) {
    con <- DBI::dbConnect(RClickhouse::clickhouse(), dbname = "litanai")
  }
  tryCatch({
    dbGetQuery(con, "SELECT 1")
    return(con)
  }, error = function(e) {
    cat("Reconnecting due to error: ", e$message, "\n")
    con <- DBI::dbConnect(RClickhouse::clickhouse(), dbname = "litanai")
    return(con)
  })
}

# --- Search Term Parsing ---
gl_searchterms_cbn <- function(text_input) {
    if (nchar(text_input) > 0) {
        l_split <- trimws(strsplit(text_input, ",")[[1]])
        print(sprintf("l_split: %s", l_split))
        purrr::map(l_split, ~{
            split_term <- unlist(strsplit(.x, ">|<|="))
            term <- trimws(split_term[1])
            frequency <- ifelse(length(split_term) > 1,
                                sprintf('%s%s', ifelse(grepl(">", .x), ">",
                                                ifelse(grepl("<", .x), "<", "=")),
                                        split_term[2]), ">0")
            list(split = term, term = sprintf('%%%s%%', term), frequency = frequency)
        })
    } else {
        list(list(split = "regression", term = "%regression%", freq = ">0"),
             list(split = "ooo", term="%ooo%", freq = ">0"))
    }
}

# --- Query Building and Data Fetching ---
sanitize_col_name <- function(name) {
  sanitized_name <- iconv(name, from = "UTF-8", to = "ASCII//TRANSLIT")
  sanitized_name <- gsub("\\s+", "_", sanitized_name) # Corrected escaping for \s
  sanitized_name <- gsub("[^a-zA-Z0-9_]", "_", sanitized_name)
  return(sanitized_name)
}

gc_qry_fmt <- function(l_searchterms_cbn) {
    search_terms_fmt <- purrr::map(l_searchterms_cbn, ~sprintf("text ilike '%s'", .x$term))
    qry_cnts <- purrr::map(l_searchterms_cbn, ~sprintf("countSubstringsCaseInsensitive(text, '%s') as cnt_%s",
                                             .x$split, sanitize_col_name(.x$split)))
    qry_cnt_filter <- purrr::map(l_searchterms_cbn, ~sprintf(" cnt_%s %s", sanitize_col_name(.x$split), .x$freq))

    sprintf("select key, length(text) as textlen, %s from littext where %s and %s",
            paste0(qry_cnts, collapse = " , "),
            paste0(search_terms_fmt, collapse = ' AND '),
            paste0(qry_cnt_filter, collapse = ' AND '))
}

bm25 <- function(dt, k1 = 1.5, b = 0.75) {
    l_count_vars <- grep("^cnt_", names(dt), value = TRUE)
    dt <- cbind(dt[, .(key)], dt[, map(.SD, as.numeric), .SDcols = c(l_count_vars, "textlen")])
    avgdl <- dt[, mean(textlen)]
    dt[, bm25 := Reduce(`+`, (map(.SD, ~((.x * (k1 + 1))/(.x + k1 *((1-b) + b*(textlen/avgdl))))))), .SDcols = l_count_vars]
    dt[order(-bm25)]
}

gd_res <- function(qry_fmt) {
    con <- check_connection(con)
    dt_res <- con %>% tbl(dbplyr::sql(qry_fmt)) %>% collect() %>% as.data.table()
    dt_bm25 <- dt_res %>% copy %>% bm25 %>% .[order(-bm25), .SD] %>%
        .[, bm25 := as.character(round(bm25, 3))]
    l_vrbls_tochar <- keep(names(dt_bm25), ~grepl("^cnt_|", .x)) # Corrected: removed extra pipe
    dt_bm25[, (l_vrbls_tochar) := map(.SD, as.character), .SDcols = l_vrbls_tochar]
    return(dt_bm25)
}


# --- Snippet Logic ---
gd_snippets_from_db <- function(db_con, doc_ids, search_term, len_pre = 40, len_post = 40) {
    if (length(doc_ids) == 0 || !is.character(doc_ids) || nchar(search_term) == 0) {
        return(data.table(key = character(), snippet = character()))
    }
    l_search_terms <- gl_searchterms_cbn(search_term) %>% map(~.x$split)
    regex_terms <- paste0(l_search_terms, collapse = "|")
    pattern <- paste0("(?i)(.{0,", len_pre, "})((?:", regex_terms, "))(.{0,", len_post, "})")
    doc_filter_clauses <- paste0("text ILIKE '%", l_search_terms, "%'", collapse = " AND ")

    query <- glue::glue_sql(
                       "SELECT
            key,
            arrayStringConcat(snippet) AS snippet
        FROM ( 
            SELECT
                key,
                arrayDistinct(extractAllGroupsVertical(text, {pattern})) AS snippets
            FROM litanai.littext
            WHERE key IN ({doc_ids*})
                AND ({DBI::SQL(doc_filter_clauses)})
        )
        ARRAY JOIN snippets AS snippet",        
        .con = db_con
    )
    dt_results <- dbGetQuery(db_con, query) %>% as.data.table() %>% .[, snippet := gsub("\\n", " ", snippet)]

    if (nrow(dt_results) > 0 && length(l_search_terms) > 1) {
        l_grepl <- purrr::map(l_search_terms, ~ grepl(.x, dt_results$snippet, ignore.case = TRUE))
        final_filter <- Reduce(`&`, l_grepl)
        dt_results <- dt_results[final_filter]
    }
    return(dt_results)
}

# --- Semantic Ordering Logic ---
gd_snippets_ordered <- function(dt_snippets, example_query, model) {
    if (is.null(example_query) || nchar(example_query) == 0 || nrow(dt_snippets) == 0) {
        return(dt_snippets)
    }
    query_embedding <- model$encode(example_query)
    snippet_embeddings <- model$encode(dt_snippets$snippet)
    cosine_sim <- function(a, b) {
        as.vector(a %*% b / (sqrt(rowSums(a^2)) * sqrt(sum(b^2))))
    }
    similarities <- cosine_sim(snippet_embeddings, query_embedding)
    dt_snippets[, similarity := similarities]
    dt_ordered <- dt_snippets[order(-similarity)]
    dt_ordered[, similarity := round(similarity, 3)]
    return(dt_ordered)
}
