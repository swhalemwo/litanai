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
            fluidRow(
                column(8, style='padding-right:2px;',
                    textInput("semantic_query", "Order by Example:", value = "")
                ),
                column(4, style='padding-left:2px;',
                    actionButton("order_button", "Order", style = "margin-top: 25px;")
                )
            ),
            hr(),
            h4("Snippet Summary"),
            tableOutput("snippet_summary_table")
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
                    uiOutput("snippet_header"),
                    tableOutput("snippets_table")
                )
            )
        )
    )
)
