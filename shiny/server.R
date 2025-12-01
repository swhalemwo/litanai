library(shiny)
library(ggplot2)
library(dplyr)
library(data.table)
library(RClickhouse)
library(dbplyr)
library(purrr)


# At the top of your server.R or in global.R
library(reticulate)
use_virtualenv("~/litanai", required = TRUE)



SentenceTransformer <- import("sentence_transformers")$SentenceTransformer
model <- SentenceTransformer("all-MiniLM-L6-v2")


# All the complexity of tokenization, running the model,
# and pooling is handled by this single line:
## embeddings <- model$encode(c("This is a sentence.", "This is another one."))



library(DBI)
con <- DBI::dbConnect(RClickhouse::clickhouse(), dbname = "litanai")
## nice, works             
## t_littext <- tbl(con, 'littext')

## server <- function(input, output) {

##     ## print(format("input: %s", input))
##     print(str(input))
##     ## qry <- t_littext %>% dplyr::filter(text %ilike% format('%%s%', input$search))

##     ## dt_res <- qry %>% collect() %>% select(key)
##     ## show_query(qry)
    
## }

check_connection <- function(con) {
  if (!dbIsValid(con)) {
    # If the connection is not valid, reconnect
    con <- DBI::dbConnect(RClickhouse::clickhouse(), dbname = "litanai")
  }
  
  # Test the connection with a simple query
  tryCatch({
    dbGetQuery(con, "SELECT 1")
    return(con) # Return the valid connection
  }, error = function(e) {
    # If there is an error, reconnect again
    cat("Reconnecting due to error: ", e$message, "\n")
    con <- DBI::dbConnect(RClickhouse::clickhouse(), dbname = "litanai")
    return(con)
  })
}

gl_searchterms_cbn <- function(text_input) {
    #' split comma-separated search terms list of terms
    #' e.g. "foo,bar" or "foo>5,bar>10" into c("foo", "bar") and extract l_freqs
    #' also supports "<" and "=" operators

    if (nchar(text_input) > 0) {
        l_split <- trimws(strsplit(text_input, ",")[[1]])
        print(sprintf("l_split: %s", l_split))

        l_terms_cbn <- purrr::map(l_split, ~{
            split_term <- unlist(strsplit(.x, ">|<|="))
            term <- trimws(split_term[1])
            frequency <- ifelse(length(split_term) > 1,
                                sprintf('%s%s', ifelse(grepl(">", .x), ">",
                                                ifelse(grepl("<", .x), "<", "=")),
                                        split_term[2]), ">0")
            list(split = term, term = sprintf('%%%s%%', term), frequency = frequency)
        })

        ## l_terms2 <- purrr::map_chr(l_terms, "term")
        ## l_freqs <- purrr::map_chr(l_search_terms, "frequency")
        
        ## return(list(l_terms = l_terms, l_freqs = l_freqs))
        return(l_terms_cbn)

    } else {
        l_terms_cbn <- list(list(split = "regression", term = "%regression%", freq = ">0"),
                            list(split = "ooo", term="%ooo%", freq = ">0"))


        return(l_terms_cbn)
    }
}


sanitize_col_name <- function(name) {
  # Transliterate characters like ö, ä, etc., to their ASCII equivalents (oe, ae)
  # The from="UTF-8" is good practice to ensure correct interpretation of the input string.
  sanitized_name <- iconv(name, from = "UTF-8", to = "ASCII//TRANSLIT")
  
  # Replace spaces with underscores first
  sanitized_name <- gsub("\\s+", "_", sanitized_name)
  
  # Replace any character that is NOT a letter, number, or underscore with an underscore
  sanitized_name <- gsub("[^a-zA-Z0-9_]", "_", sanitized_name)
  
  return(sanitized_name)
}


gc_qry_fmt <- function(l_searchterms_cbn) {

    
    ## l_terms <- l_searchterms_cnt %>% chuck("l_terms")
    ## l_freqs <- l_searchterms_cnt %>% chuck("l_freqs")



    ## qry_reduced <- reduce(l_search_terms, \(x,y) sprintf("text %%ilike%% %s and", x))

    ## print(sprintf("l_search_terms: %s", l_search_terms))
    search_terms_fmt <- purrr::map(l_searchterms_cbn, ~sprintf("text ilike '%s'", .x$term))

    print(len(search_terms_fmt))
    print(sprintf("search_terms_fmt: %s", search_terms_fmt))
    ## qry <- t_littext %>%
    ##     dplyr::filter(
    ##                reduce(l_search_terms, \(acc, term) acc | (text %ilike% term)))


    qry_cnts <- purrr::map(l_searchterms_cbn, ~sprintf("countSubstringsCaseInsensitive(text, '%s') as cnt_%s",
                                             .x$split, sanitize_col_name(.x$split)))

    qry_cnt_filter <- purrr::map(l_searchterms_cbn, ~sprintf(" cnt_%s %s", sanitize_col_name(.x$split), .x$freq))

    qry_fmt <- sprintf("select key, length(text) as textlen, %s from littext where %s and %s",
                       paste0(qry_cnts, collapse = " , "),
                       paste0(search_terms_fmt, collapse = ' AND '),
                       paste0(qry_cnt_filter, collapse = ' AND ')
                       )
    print(sprintf("qry_fmt: %s", qry_fmt))
    return(qry_fmt)
}






bm25 <- function(dt, k1 = 1.5, b = 0.75) {
    ## Identify count columns
    l_count_vars <- grep("^cnt_", names(dt), value = TRUE)
    
    
    ## https://en.wikipedia.org/wiki/Okapi_BM25
    ## dt_idf not needed here: every term occus in every document
    ## N <- dt[, .N]
    ## dt_idf <- dt[, map(.SD, ~(log(((N -  len(.x) + 0.5)/(len(.x) + 0.5)) + 1))), .SDcols = l_count_vars]

    dt <- cbind(dt[, .(key)], dt[, map(.SD, as.numeric), .SDcols = c(l_count_vars, "textlen")])
    avgdl <- dt[, mean(textlen)]

    dt[, bm25 := Reduce(`+`, (map(.SD, ~((.x * (k1 + 1))/(.x + k1 *((1-b) + b*(textlen/avgdl))))))), .SDcols = l_count_vars]

    ## l_bm25_prep <- paste0("bm25prep_", l_count_vars)
    ## l_bm25_prep_num <- paste0("b25_num_", l_count_vars)
    ## l_bm25_prep_denum <- paste0("b25_denum_", l_count_vars)
    
    ## dt2 <- dt %>% copy
    ## dt <- dt2[, c(list(key, map(.SD, as.numeric))), .SDcols = c(l_count_vars, "textlen")]

    

    ## dt[, (l_bm25_prep_num) := map(.SD, ~((.x*1.001) * (k1 + 1))), .SDcols = l_count_vars]
    ## dt[, (l_bm25_prep_denum) := map(.SD, ~.x + k1 *((1-b) + b*(textlen/avgdl))), .SDcols = l_count_vars]

    ## dt[, (l_bm25_prep) := map(.SD, ~((.x * (k1 + 1))/(.x + k1 *((1-b) + b*(textlen/avgdl))))), .SDcols = l_count_vars]

    dt[order(-bm25)]


    ## dt[key %in% c("Aengenheyster_2024_structural", "Eikenberry_2006_governance", "Barman_2017_philanthropy"),
    ##    .SD, .SDcols = c("key", l_bm25_prep_num, l_count_vars)]

    
    ## dt <- dt[order(-dt$bm25_score), ]  # Sort by BM25 score in descending order
    return(dt)
}




## bm25(data.table(key = c('j', 'k'), textlen = c(75896,90000), cnt_x = c(1, 3), cnt_y = c(1, 4)))

# Example call to the function
## avg_doc_len <- mean(dt_res$textlen)  # Calculate average document length from your data
## dt_res <- bm25(dt_res, avg_doc_len = avg_doc_len)



#' Fetch search snippets from the littext database using the extractAll method.
#'
#' @param db_con A DBI database connection object to ClickHouse.
#' @param doc_ids A character vector of document keys to search within.
#' @param search_term The term to search for.
#' @return A data.frame with the key and the corresponding snippet, with the search term highlighted.
gd_snippets_from_db <- function(db_con, doc_ids, search_term, len_pre = 40, len_post = 40) {
    ## Return empty frame if there's nothing to search
    if (length(doc_ids) == 0 || !is.character(doc_ids) || nchar(search_term) == 0) {
        return(data.frame(key = character(), snippet = character()))
    }

    ## The user's search term needs to be escaped for the regex, but not for multiSearchAny
    l_search_terms <- gl_searchterms_cbn(search_term) %>% map(~chuck(.x, "split"))

    # Create a regex pattern to extract snippets containing ANY of the search terms.
    regex_terms <- paste0(l_search_terms, collapse = "|")
    pattern <- paste0("(?i)(.{0,", len_pre, "})((?:", regex_terms, "))(.{0,", len_post, "})")

    # Create the AND clauses for filtering the documents to ensure they contain ALL terms.
    doc_filter_clauses <- paste0("text ILIKE '%", l_search_terms, "%'", collapse = " AND ")

    # Construct the final SQL query
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
                -- Filter for documents containing ALL search terms
                AND ({DBI::SQL(doc_filter_clauses)})
        )
        -- Expand the array of snippets into individual rows.
        ARRAY JOIN snippets AS snippet",
        .con = db_con
    )
    cat("Executing Snippet Query:\n", query, "\n")
    dt_results <- dbGetQuery(db_con, query) %>% adt %>% .[, snippet := gsub("\n", " ", snippet)]

    # --- Post-filter snippets in R to ensure all terms are present ---
    # This is the crucial step to enforce the AND logic within each snippet itself.
    if (nrow(dt_results) > 0 && length(l_search_terms) > 1) {
        # Create a list of logical vectors, one for each search term
        l_grepl <- purrr::map(l_search_terms, ~ grepl(.x, dt_results$snippet, ignore.case = TRUE))
        
        # Reduce the list with '&' to find rows where all terms are present
        final_filter <- Reduce(`&`, l_grepl)
        
        # Apply the filter
        dt_results <- dt_results[final_filter]
    }

    return(dt_results)
}



#' Helper function to merge overlapping intervals.
#' Expects a data.table with columns: term, start, end.
#' Returns a data.table with merged intervals and a list of terms contained in each.
merge_intervals <- function(dt_intervals) {
    if (nrow(dt_intervals) == 0) {
        return(data.table(start = integer(), end = integer(), terms = list()))
    }

    # Order by start position
    setorder(dt_intervals, start)

    # Initialize with the first interval
    merged <- list()
    current_start <- dt_intervals[1, start]
    current_end <- dt_intervals[1, end]
    current_terms <- as.character(dt_intervals[1, term])

    # Loop through the rest of the intervals
    for (i in 2:nrow(dt_intervals)) {
        next_interval <- dt_intervals[i]
        
        # Check for overlap
        if (next_interval$start <= current_end) {
            # Merge by extending the end and adding unique terms
            current_end <- max(current_end, next_interval$end)
            current_terms <- union(current_terms, as.character(next_interval$term))
        } else {
            # No overlap, so the current merged interval is complete
            merged <- append(merged, list(list(start = current_start, end = current_end, terms = current_terms)))
            
            # Start a new merge
            current_start <- next_interval$start
            current_end <- next_interval$end
            current_terms <- as.character(next_interval$term)
        }
    }

    # Add the last merged interval
    merged <- append(merged, list(list(start = current_start, end = current_end, terms = current_terms)))

    # Convert list of lists to a data.table
    rbindlist(merged)
}


#' Fetch snippets using a "chain of proximity" logic (v2).
#'
#' This function finds documents with all terms, then finds all occurrences
#' of those terms, merges their "influence ranges", and extracts snippets
#' from merged ranges that contain all unique search terms.
gd_snippets_from_db_v2 <- function(db_con, doc_ids, search_term, len_pre = 40, len_post = 40) {
    if (length(doc_ids) == 0 || !is.character(doc_ids) || nchar(search_term) == 0) {
        return(data.table(key = character(), snippet = character()))
    }

    l_search_terms <- gl_searchterms_cbn(search_term) %>% map_chr(~.x$split)
    doc_filter_clauses <- paste0("text ILIKE '%", l_search_terms, "%'", collapse = " AND ")

    # Query to get the full text and an array of all positions for each search term
    pos_query <- glue::glue_sql(
        "SELECT
            key,
            text,
            arrayMap(term -> positions(lowerUTF8(text), lowerUTF8(term)), {l_search_terms}) AS term_positions
        FROM litanai.littext
        WHERE key IN ({doc_ids*})
          AND ({DBI::SQL(doc_filter_clauses)})",
        .con = db_con
    )

    cat("Executing Position Query:\n", pos_query, "\n")
    dt_positions <- dbGetQuery(db_con, pos_query) %>% as.data.table()

    if (nrow(dt_positions) == 0) {
        return(data.table(key = character(), snippet = character()))
    }

    final_snippets <- list()

    # Process each document
    for (i in 1:nrow(dt_positions)) {
        doc_row <- dt_positions[i, ]
        doc_key <- doc_row$key
        doc_text <- doc_row$text
        term_positions <- doc_row$term_positions[[1]] # Comes as a list of lists

        # Create a flat data.table of all term occurrences and their influence ranges
        l_intervals <- purrr::map2(l_search_terms, term_positions, function(term, positions) {
            if (length(positions) > 0) {
                data.table(
                    term = term,
                    start = pmax(1, positions - len_pre),
                    end = positions + nchar(term) + len_post
                )
            }
        })
        dt_intervals <- rbindlist(l_intervals)

        if (nrow(dt_intervals) > 0) {
            # Merge these intervals
            dt_merged <- merge_intervals(dt_intervals)

            # Filter for merged intervals that contain ALL search terms
            dt_valid <- dt_merged[sapply(terms, function(t) all(l_search_terms %in% t))]

            if (nrow(dt_valid) > 0) {
                # Extract the snippets from the text
                doc_snippets <- dt_valid[, .(
                    key = doc_key,
                    snippet = substring(doc_text, start, end)
                )]
                final_snippets <- append(final_snippets, list(doc_snippets))
            }
        }
    }

    if (length(final_snippets) > 0) {
        return(rbindlist(final_snippets))
    } else {
        return(data.table(key = character(), snippet = character()))
    }
}

# --- Example Usage ---
# This is how you would call the function from your Shiny server logic.

# 1. Establish connection to your database (replace with your actual connection code)

## ## 2. Define the inputs based on your example
## example_doc_id <- c("FilippiMazzola_Wit_2024_neural.pdf", "Bianchi_etal_2024_rem.pdf")
## ## example_doc_id <- "Kirk_2017_trove.pdf"
## example_search_term <- "regression,likelihood,event"
## ## example_search_term <- c("vision,unique")
## ## # 3. Call the function to get snippets
## ## # In a real app, 'con' would be your live database connection

## ## here it works for 2 terms
## example_search_term <- "regression,likelihood"
## dt_snippets <- gd_snippets_from_db_v2(con, example_doc_id, example_search_term, len_pre = 200, len_post = 200)
## dt_snippets[, snippet]

## ## but for 3 it doesn't work: relational is in 2 of the previously generated snippets, so they should be there
## example_search_term <- "regression,likelihood,relational"
## dt_snippets <- gd_snippets_from_db(con, example_doc_id, example_search_term, len_pre = 200, len_post = 200)
## dt_snippets[, snippet]



#' Order snippets based on semantic similarity to an example query.
#'
#' @param dt_snippets A data.table containing a 'snippet' column.
#' @param example_query A string to compare against.
#' @param model A sentence-transformer model object.
#' @return A data.table ordered by similarity, with a new 'similarity' column.
gd_snippets_ordered <- function(dt_snippets, example_query, model) {
    # Return original table if query is empty or snippets are empty
    if (is.null(example_query) || nchar(example_query) == 0 || nrow(dt_snippets) == 0) {
        return(dt_snippets)
    }

    # Generate embeddings
    query_embedding <- model$encode(example_query)
    snippet_embeddings <- model$encode(dt_snippets$snippet)

    # Helper function for cosine similarity
    # Calculates cosine similarity between a matrix `a` and a vector `b`
    cosine_sim <- function(a, b) {
        as.vector(a %*% b / (sqrt(rowSums(a^2)) * sqrt(sum(b^2))))
    }
    
    similarities <- cosine_sim(snippet_embeddings, query_embedding)

    # Add similarity score to data.table and order
    dt_snippets[, similarity := similarities]
    dt_ordered <- dt_snippets[order(-similarity)]
    
    # Format the similarity column for better display
    dt_ordered[, similarity := round(similarity, 3)]

    return(dt_ordered)
}

## dt_snippets <- gd_snippets_from_db(con, example_doc_id, example_search_term, len_pre = 200, len_post = 200)
## example_query <- "cox regression allows for non-parametric identification of survival predictors"
## gd_snippets_ordered(dt_snippets, example_query,  model)






gd_res <- function(qry_fmt) {

    con <- check_connection(con)
    dt_res <- con %>% tbl(dbplyr::sql(qry_fmt)) %>% collect() %>% adt

    dt_bm25 <- dt_res %>% copy %>% bm25 %>% .[order(-bm25), .SD] %>%
        .[, bm25 := as.character(round(bm25, 3))]

    ## set up some variable vectors
    l_vrbls_tochar <- keep(names(dt_bm25), ~grepl("^cnt_|textlen|", .x)) # which to convert: only cnts, text
    ## l_vrbls_cnt <- keep(names(dt_bm25), ~grepl("^cnt_", .x)) # 
    ## l_vrbls_prop <- gsub("^cnt_", "prop_", l_vrbls_cnt)


    ## dt_res[, (l_vrbls_prop) := map(.SD, ~.x/textlen), .SDcols = l_vrbls_cnt] # calculate props
    ## dt_res[, rank_prop := rowSums(.SD), .SDcols = l_vrbls_prop] # calculate rank

    ## convert to char, somehow needed for display FIXME
    dt_char <- dt_bm25[, (l_vrbls_tochar) := map(.SD, as.character), .SDcols = l_vrbls_tochar]
    ## dt_res2 <- dt_res[order(-rank_prop), .SD, .SDcols = c("key", l_vrbls_cnt)] # select relevant ones
    
    

    return(dt_char)
}



## gl_searchterms_cbn("philanthro>5,innovat>5") %>% gc_qry_fmt %>% gd_res

## gc_qry_fmt("foo>5,bar,baz")
## gc_qry_fmt("")
## gl_searchterms_cbn("elite philanthropy,innovat") %>% gc_qry_fmt %>% gd_res()




server <- function(input, output) {
    
    doc_search_results <- reactiveVal(data.table())
    snippet_results <- reactiveVal(data.table())
 

    # Display the entered search string
    output$search_result <- renderUI({
        req(input$search)  # Require input to be available
        HTML(paste("Search String: <strong>", input$search, "</strong></br>"))
    })
    
    # Observe when input$search changes and perform query
    observeEvent(input$search, {
        print(input$search)
        l_searchterms_cbn <- gl_searchterms_cbn(input$search)
        qry_fmt <- gc_qry_fmt(l_searchterms_cbn)
        dt_res <- gd_res(qry_fmt)
        doc_search_results(dt_res)
        output$results_table <- renderTable({dt_res})
    })

    # Observe when the snippet search term changes
    observeEvent(input$snippet_search, {
        req(input$snippet_search, doc_search_results())
        l_doc_keys <- doc_search_results()$key
        con <- check_connection(con)

        if (length(l_doc_keys) > 0) {
            # Call the new v2 function for snippet generation
            dt_snippets <- gd_snippets_from_db_v2(con,
                                                l_doc_keys,
                                                input$snippet_search,
                                                len_pre = input$len_pre,
                                                len_post = input$len_post)
            # Store the raw snippets
            snippet_results(dt_snippets)
            # Display the unordered snippets
            output$snippets_table <- renderTable({ dt_snippets })
        } else {
            snippet_results(data.table()) # Clear if no docs
            output$snippets_table <- renderTable({ data.table() })
        }
    })

    # Observe when the order button is clicked
    observeEvent(input$order_button, {
        dt_snips <- snippet_results()
        semantic_q <- input$semantic_query

        # Only proceed if there are snippets and a query
        if (nrow(dt_snips) > 0 && !is.null(semantic_q) && nchar(semantic_q) > 0) {
            # Show a notification that ordering is in progress
            showNotification("Ordering snippets by semantic similarity...", duration = 3, type = "message")
            
            dt_ordered <- gd_snippets_ordered(copy(dt_snips), semantic_q, model)
            output$snippets_table <- renderTable({ dt_ordered })
        }
    })

    output$snippet_header <- renderUI({
        n_snippets <- nrow(snippet_results())
        
        if (n_snippets > 0) {
            title <- sprintf("Snippet Search Results (%d)", n_snippets)
        } else {
            title <- "Snippet Search Results"
        }
        h4(title)
    })

    output$snippet_summary_table <- renderTable({
        dt_snips <- snippet_results()
        if (nrow(dt_snips) > 0) {
            summary_table <- dt_snips[, .N, by = key][order(-N)]
            setnames(summary_table, c("Document", "Snippets"))
            return(summary_table)
        }
        return(NULL)
    })
}








