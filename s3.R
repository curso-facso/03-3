Sys.setenv("AWS_ACCESS_KEY_ID" = "DJW1eSkJgmGZ42XV45oH", # enter your credentials
           "AWS_SECRET_ACCESS_KEY" = "ja5B4ABel4hZCZ4W0AzvBsdPKqygfLzFrKThV80J", # enter your credentials
           "AWS_S3_ENDPOINT" = "192.168.1.92:9000") 


library(arrow)
library(aws.s3)
library("minio.s3")


b <- get_bucket(bucket = 'arrow-twitter' )
ob <- aws.s3::s3read_using(FUN = read.csv, object = "2022_01/2022_01_01_00_Summary_Details.csv", bucket = b, opts = list(use_https = FALSE, region = ""))


minio_key <- Sys.getenv("MINIO_ACCESS_KEY", "DJW1eSkJgmGZ42XV45oH")
minio_secret <- Sys.getenv("MINIO_SECRET_KEY", "ja5B4ABel4hZCZ4W0AzvBsdPKqygfLzFrKThV80J")
minio_host <- Sys.getenv("MINIO_HOST", "192.168.1.92")
minio_port <- Sys.getenv("MINIO_PORT", "9000")
minio_arrow_bucket <- Sys.getenv("MINIO_ARROW_BUCKET", "arrow-twitter")
minio_path <- function(...) paste(minio_arrow_bucket, ..., sep = "/")


minio_uri <- function(...) {
  template <- "s3://%s:%s@%s?scheme=http&endpoint_override=%s%s%s"
  sprintf(template, minio_key, minio_secret, minio_path(...), minio_host, ":", minio_port)
}

data <- open_dataset(minio_uri("2022_01"), format = "csv" )

library(dplyr)

data %>% 
  #group_by(COMUNA) %>% 
  summarise(frecuencia = n()) %>% 
  collect()


data %>%
  group_by(Country) %>%
  write_dataset(minio_uri("2022-01_parquet"), format = "parquet" )

data <- open_dataset(minio_uri("arrow-twitter"), format = "csv" )


bucket <- 'arrow-twitter'
s3_path <- "s3://arrow-twitter/2022_01/2022_01_01_00_Summary_Details.csv"
data <- arrow::read_csv_arrow(s3_path)


b <- get_bucket(bucket = 'arrow-twitter', use_https = F, region = "")


b <- get_bucket(bucket = 'arrow-twitter', use_https = F)






library(arrow)
arrow_with_s3()
