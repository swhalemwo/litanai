library(shiny)
library(ggplot2)
library(dplyr)
library(data.table)

# Source the business logic from the separate file
source("logic.R")

# At the top of your server.R or in global.R
library(reticulate)
use_virtualenv("~/litanai", required = TRUE)

SentenceTransformer <- import("sentence_transformers")$SentenceTransformer
model <- SentenceTransformer("all-MiniLM-L6-v2")

# Establish a global database connection for the app session
# Note: In a multi-user environment, managing connections on a per-session
# basis within the server function is a more robust pattern.
library(DBI)
con <- DBI::dbConnect(RClickhouse::clickhouse(), dbname = "litanai")


# --- Shiny Server Function ---
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
        # Re-establish connection if it's not valid
        con <<- check_connection(con)
        dt_res <- gd_res(qry_fmt)
        doc_search_results(dt_res)
        output$results_table <- renderTable({dt_res})
    })

    # Observe when the snippet search term changes
    observeEvent(input$snippet_search, {
        req(input$snippet_search, doc_search_results())
        l_doc_keys <- doc_search_results()$key
        # Re-establish connection if it's not valid
        con <<- check_connection(con)

        if (length(l_doc_keys) > 0) {
            dt_snippets <- gd_snippets_from_db(con,
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








