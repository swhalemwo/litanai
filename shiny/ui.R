library(shiny)
library(ggplot2)
library(dplyr)
library(data.table)

ui <- fluidPage(
    titlePanel("Search Functionality in Shiny"),
    
    sidebarLayout(
        sidebarPanel(
            textInput("search", "Document Search:", value = ""),
            textInput("snippet_search", "Snippet Search (placeholder):", value = "")
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
                    # Placeholder for future snippet UI
                    h4("Snippet Search"),
                    p("This tab will be used to display snippets from selected documents.")
                )
            )
        )
    )
)
