library(dplyr)
library(arrow)
library(readr)
library(stringr)
library(purrr)



read_and_write <- function(source_path, source_file, destination_path ) {
    original_file <- read_csv( paste0(source_path, "/", source_file), show_col_types = F )
    
    name_nyew_file <- str_remove(pattern =   "csv", string =   source_file) 
    name_nyew_file <- paste0(name_nyew_file, "parquet")
    
    destination_file <- paste0(destination_path, "/", name_nyew_file)
    write_parquet(original_file, destination_file)
}

# Primer mes
csv_files <-  list.files("data/twitter/archive/Summary_Details/2022_01/", full.names = F)
walk(csv_files, ~read_and_write("data/twitter/archive/Summary_Details/2022_01", .x, "data/twitter/archive-parquet/Summary_Details/2022_01-parquet" ))
length(list.files("data/twitter/archive/Summary_Details/2022_01/"))
length(list.files("data/twitter/archive-parquet/Summary_Details/2022_01-parquet/"))

# Segundo mes
csv_files <-  list.files("data/twitter/archive/Summary_Details/2022_02/", full.names = F)
walk(csv_files, ~read_and_write("data/twitter/archive/Summary_Details/2022_02", .x, "data/twitter/archive-parquet/Summary_Details/2022_02-parquet" ))
length(list.files("data/twitter/archive/Summary_Details/2022_02/"))
length(list.files("data/twitter/archive-parquet/Summary_Details/2022_02-parquet/"))


# Tercer mes
csv_files <-  list.files("data/twitter/archive/Summary_Details/2022_03/", full.names = F)
walk(csv_files, ~read_and_write("data/twitter/archive/Summary_Details/2022_03", .x, "data/twitter/archive-parquet/Summary_Details/2022_03-parquet" ))
length(list.files("data/twitter/archive/Summary_Details/2022_03/"))
length(list.files("data/twitter/archive-parquet/Summary_Details/2022_03-parquet/"))

