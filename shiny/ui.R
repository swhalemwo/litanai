library(shiny)
library(ggplot2)
library(dplyr)
library(data.table)

ui <- fluidPage(
    titlePanel("Search Functionality in Shiny"),
    
    sidebarLayout(
        sidebarPanel(
            textInput("search", "Document Search:", value = ""),
            hr(),
            h4("Snippet Options"),
            textInput("snippet_search", "Snippet Search:", value = ""),
            numericInput("len_pre", "Context Before (chars):", value = 50, min = 0, max = 500),
            numericInput("len_post", "Context After (chars):", value = 50, min = 0, max = 500),
            hr(),
            h4("Semantic Ordering"),
            textInput("semantic_query", "Order by Example:", value = "")
        ),
        
        mainPanel(
            tabsetPanel(
                id = "main_tabs",
                tabPanel(
                    "Document Search",
                    uiOutput("search_result"),
                    tableOutput("results_table")
                ),
                tabPanel(
                    "Snippet Search",
                    h4("Snippet Search Results"),
                    tableOutput("snippets_table")
                )
            )
        )
    )
)
