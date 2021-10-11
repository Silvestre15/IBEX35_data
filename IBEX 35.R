# Load the necessary libraries
library(tidyverse)
library(rvest)
library(lubridate)
library(scales)
library(git2r)

#### PARAMETERS ####

# Website from which you want to retrieve the data
Website <- "https://www.investing.com/indices/spain-35-components"

# Date and hour format you prefer
date_format <- "%d-%m-%Y" 
time_format <- "%H:%M:%S"

# GIT parameters
git2r::config(user.name = "Silvestre15",user.email = "silvestredim@gmail.com")

git2r::status()


# Location and name of csv where the scrapped data will be stored.
# This serves as a step for converting the data displayed on the 
# web into the final data that we will use to do our analysis
path_name1 <- "~/Dropbox/R/IBEX 35 R/live_var.csv"

# Location and name of the csv where the final data will be stored
# This is the important file that will be divided further on
path_name2 <- "~/Dropbox/R/IBEX 35 R/final_data.csv"

# Time lapse between execution of the function that will scrape and
# add the new rows to the csv (seconds)
lapse <- 5

#### DATA WRANGLING ####

# Create the empty data frame with the column names (in this case the 
# stock's names). For that we must scrape a first time the web in order 
# to get the column names

## Fetch stocks' quotes from website
web <- read_html(Website)

## Transform into tables
tables <- web %>% html_table()

# Select desired table and make the adequate changes to simplify next steps
Investing_df <- tables[[2]] %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    )
  ) %>%
  rename(Var_pct=`Chg. %`) %>%
  dplyr::select(Name, Var_pct) %>%
  pivot_wider(names_from = Name, values_from = Var_pct) %>%
  mutate(Time=as.character(Sys.time(), time_format), .before=1) %>%
  mutate(Date=as.character(Sys.Date(), date_format), .before=1)

# Create the empty df to accumulate the stocks' variations inside
columns <- colnames(Investing_df)

## Create a df with 0 rows and x number of columns
live_var <- data.frame(matrix(nrow = 0, ncol = length(columns)))

## Assign column names
colnames(live_var)=columns

# Export the file that will be imported later to fill it
write_csv(live_var, path_name1)

# Now we can run the function that will constantly scrape the web page
# and retrieve new information about the variation of the stocks
# Contributed by @QHarr
get_row <- function(){
  
  web <- read_html(Website)
  
  table <- web %>% html_node('#cr1') %>% html_table()
  
  investing_df <- table %>%
    dplyr::select(
      where(
        ~sum(!is.na(.x)) > 0
      )
    ) %>%
    rename(var_pct=`Chg. %`) %>%
    dplyr::select(Name, var_pct) %>%
    pivot_wider(names_from = Name, values_from = var_pct) %>%
    mutate(Time=as.character(Sys.time(), time_format), .before=1) %>%
    mutate(Date=as.character(Sys.Date(), date_format), .before=1)
    return(investing_df)
}

# We now have to bind together the previously created template with
# the new information from the function get_row
write_data <- function(){
  
## First turn the function into a data frame
new_data <- get_row()

## Import the previously exported data in order to write the new row
exported <- read_csv(path_name1, col_types = cols(Time=col_character()))

## Write the new row
binded <- bind_rows(exported, new_data)

## Export the newly binded data frame to the same place
write_csv(binded, path_name1)
}

## First we need to import the csv and turn the Time variable into a 
## readable HMS format
modify_data <- function(){
  
## Re-import the data to convert the Time column into readable time for R
plot_data <- read.csv(path_name1)

## We must remove the percentage signs 
plot_data[3:37]<-matrix(apply(plot_data[3:37], 2, function(x) 
  as.numeric(sub("%","",as.character(x)))))

## Export once again with the last changes
write_csv(plot_data, path_name2)
}


# Separate the data into various csv files in order to better read it
# in Python
## There must be a better way of doing the following, but I'm an ignorant
## so for now I'll have to deal with this
python_csv <- function(){
  
date_time <- read.csv(path_name2) %>%
    select(1:2)
  
stocks <- read.csv(path_name2) %>%
    select((3:37)) %>%
    split.default(rep(1:35, each=1))

export_csv <- bind_cols(date_time, stocks[["1"]]) %>%
  rename(var=Acciona) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Acciona_var.csv")

export_csv <- bind_cols(date_time, stocks[["2"]]) %>%
  rename(var=Acerinox) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Acerinox_var.csv")

export_csv <- bind_cols(date_time, stocks[["3"]]) %>%
  rename(var=ACS) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/ACS_var.csv")

export_csv <- bind_cols(date_time, stocks[["4"]]) %>%
  rename(var=Aena) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Aena_var.csv")

export_csv <- bind_cols(date_time, stocks[["5"]]) %>%
  rename(var=Almirall) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Almirall_var.csv")

export_csv <- bind_cols(date_time, stocks[["6"]]) %>%
  rename(var=Amadeus) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Amadeus_var.csv")

export_csv <- bind_cols(date_time, stocks[["7"]]) %>%
  rename(var=ArcelorMittal) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/ArcelorMittal_var.csv")

export_csv <- bind_cols(date_time, stocks[["8"]]) %>%
  rename(var=Banco.de.Sabadell) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Banco de Sabadell_var.csv")

export_csv <- bind_cols(date_time, stocks[["9"]]) %>%
  rename(var=Bankinter) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Bankinter_var.csv")

export_csv <- bind_cols(date_time, stocks[["10"]]) %>%
  rename(var=BBVA) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/BBVA_var.csv")

export_csv <- bind_cols(date_time, stocks[["11"]]) %>%
  rename(var=Caixabank) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Caixabank_var.csv")

export_csv <- bind_cols(date_time, stocks[["12"]]) %>%
  rename(var=Cellnex.Telecom) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Cellnex Telecom_var.csv")

export_csv <- bind_cols(date_time, stocks[["13"]]) %>%
  rename(var=Cie.Automotive) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Cie Automotive_var.csv")

export_csv <- bind_cols(date_time, stocks[["14"]]) %>%
  rename(var=Enagas) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Enagas_var.csv")

export_csv <- bind_cols(date_time, stocks[["15"]]) %>%
  rename(var=Endesa) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Endesa_var.csv")

export_csv <- bind_cols(date_time, stocks[["16"]]) %>%
  rename(var=Ferrovial) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Ferrovial_var.csv")

export_csv <- bind_cols(date_time, stocks[["17"]]) %>%
  rename(var=Fluidra) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Fluidra_var.csv")

export_csv <- bind_cols(date_time, stocks[["18"]]) %>%
  rename(var=Gamesa) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Gamesa_var.csv")

export_csv <- bind_cols(date_time, stocks[["19"]]) %>%
  rename(var=Grifols) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Grifols_var.csv")

export_csv <- bind_cols(date_time, stocks[["20"]]) %>%
  rename(var=IAG) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/IAG_var.csv")

export_csv <- bind_cols(date_time, stocks[["21"]]) %>%
  rename(var=Iberdrola) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Iberdrola_var.csv")

export_csv <- bind_cols(date_time, stocks[["22"]]) %>%
  rename(var=Inditex) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Inditex_var.csv")

export_csv <- bind_cols(date_time, stocks[["23"]]) %>%
  rename(var=Indra.A) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Indra A_var.csv")

export_csv <- bind_cols(date_time, stocks[["24"]]) %>%
  rename(var=Inmobiliaria.Colonial) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Inmobiliaria Colonial_var.csv")

export_csv <- bind_cols(date_time, stocks[["25"]]) %>%
  rename(var=Mapfre) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Mapfre_var.csv")

export_csv <- bind_cols(date_time, stocks[["26"]]) %>%
  rename(var=Melia.Hotels) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Melia Hotels_var.csv")

export_csv <- bind_cols(date_time, stocks[["27"]]) %>%
  rename(var=Merlin.Properties.SA) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Merlin Properties SA_var.csv")

export_csv <- bind_cols(date_time, stocks[["28"]]) %>%
  rename(var=Naturgy.Energy) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Naturgy_var.csv")

export_csv <- bind_cols(date_time, stocks[["29"]]) %>%
  rename(var=Pharma.Mar) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Pharma Mar_var.csv")

export_csv <- bind_cols(date_time, stocks[["30"]]) %>%
  rename(var=Red.Electrica) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Red Electrica_var.csv")

export_csv <- bind_cols(date_time, stocks[["31"]]) %>%
  rename(var=Repsol) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Repsol_var.csv")

export_csv <- bind_cols(date_time, stocks[["32"]]) %>%
  rename(var=Santander) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Santander_var.csv")

export_csv <- bind_cols(date_time, stocks[["33"]]) %>%
  rename(var=Solaria) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Solaria_var.csv")

export_csv <- bind_cols(date_time, stocks[["34"]]) %>%
  rename(var=Telefonica) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Telefonica_var.csv")

export_csv <- bind_cols(date_time, stocks[["35"]]) %>%
  rename(var=Viscofan) %>%
  write.csv("~/Dropbox/Python/IBEX35_data/Viscofan_var.csv")

}

# If the code runs properly, the csv file with which we are left has the 
# necessary formats in order to properly plot further on. This means that 
# the first column displays the date, the second column the time in a 
# suitable format and the rest of the columns represent the components
# of the selected index. Those last columns represent the variations in
# PERCENTAGE (remember that we previously removed those signs)

# This is the function that does the magic because it allows us to repeat 
# the whole process over and over again every X time (defined earlier)
repeat {
  startTime <- Sys.time()
  write_data()
  print(paste0("Retrieving stock variations at ", Sys.time()))
  modify_data()
  print(paste0("Modifying data"))
  python_csv()
  print(paste0("Creating csv for Python"))
  sleepTime <- startTime + lapse - Sys.time()
  if (sleepTime > 0)
    Sys.sleep(sleepTime)
}