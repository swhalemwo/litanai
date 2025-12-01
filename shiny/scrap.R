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
            arrayMap(term -> positions(lowerUTF8(text), lowerUTF8(term)), [{l_search_terms*}]) AS term_positions
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
