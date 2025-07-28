library(shiny)
library(ggplot2)
library(dplyr)
library(data.table)


## ui <- fluidPage(
##   titlePanel("Search Functionality in Shiny"),
  
##   sidebarLayout(
##     sidebarPanel(
##       textInput("search", "Search:", value = "")
##     ),
    
##     mainPanel(
##       uiOutput("search_result")
##     )
##   )
## )


ui <- fluidPage(
    titlePanel("Search Functionality in Shiny"),
    
    sidebarLayout(
        sidebarPanel(
            textInput("search", "Search:", value = "")
        ),
        
        mainPanel(
            uiOutput("search_result"),  # Display the search string
            tableOutput("results_table")  # Display the results of the query
        )
    )
)
