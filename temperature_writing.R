# Temperature Writing to database 
##### 0.1 Packages #####

# db
library(RPostgres)

#Dplyr stuff
library(magrittr)
library(tidyverse)
library(lubridate)
library(readxl)


#Database Stuff
library(RODBC)
library(odbc)
library(pool)
`%!in%` <- Negate(`%in%`)


##### 0.2 Connect to database #####
marsDBcon <- dbPool(RPostgres::Postgres(),
               dbname = 'mars_prod', 
               host = 'PWDMARSDBS1', 
               port = 5434, 
               user = Sys.getenv("admin_uid"),
               password = Sys.getenv("admin_pwd"))

##### 0.3 Import site names of interest #####

ow_list <- ow_uid_plant_health <- read_excel("ow_uid_plant_health.xlsx") %>%
           dplyr::select(ow_uid) %>% unique %>% pull



#### 1.0 Query Postgres DB ####

access_list <- dbGetQuery(marsDBcon, paste0("SELECT * FROM admin.tbl_accessdb WHERE ow_uid in (",paste(ow_list, collapse= ", "),")"))



#### 2.0 Iterate through Access DB's and write to Postgres DB ####

for( i in 1:nrow(access_list)){
  
# # identify the ow
# ow_x <- access_list$ow_uid[i]


# read temperature data from access file
accessdbCon_x <- RODBC::odbcConnectAccess2007(access_list$filepath[i])

temp_query_x <- paste0("SELECT [",access_list$datatable[i],"].[Standard Dtime], ",
                              "[",access_list$datatable[i],"].[Temp BW (°F)] FROM [",access_list$datatable[i],"]")

temp_data_x <- sqlQuery(accessdbCon_x, temp_query_x, as.is = TRUE)


# handle datetime coming in as text and rename column
temp_data_x <- temp_data_x %>% dplyr::mutate(dtime_est = ymd_hms(`Standard Dtime`)) %>%
                               dplyr::select(-`Standard Dtime`)

# append ow_uid
temp_data_x$ow_uid <- access_list$ow_uid[i]

#rename columns
colnames(temp_data_x) <- c("temperature_degF", "dtime_est", "ow_uid")

#close access connection
odbcClose(accessdbCon_x)


# show first
head(temp_data_x)

# filter out NA values
temp_data_x <- temp_data_x %>% dplyr::filter(!is.na(temperature_degF))

# Write to postgres

write_results <- dbWriteTable(marsDBcon,
                 DBI::SQL("data.tbl_sw_temperature_fixed"),
                 temp_data_x,
                 append = TRUE,
                 row.names = FALSE)

print(write_results)
}


#### 3.0 Close it out ####
poolClose(marsDBcon)



