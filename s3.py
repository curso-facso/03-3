
import pyarrow.parquet as pq
import pyarrow.csv as pcsv
import s3fs
import os

fs = s3fs.S3FileSystem(
    anon=False,
    use_ssl=True,
    client_kwargs={
        #"region_name": os.environ['S3_REGION'],
        "endpoint_url": "http://192.168.1.92:9000",
        "aws_access_key_id": "DJW1eSkJgmGZ42XV45oH",
        "aws_secret_access_key": "ja5B4ABel4hZCZ4W0AzvBsdPKqygfLzFrKThV80J",
        "verify": True,
    }
)


#s3_filepath = "arrow-twitter/particiones/COMUNA=10101/part-0.parquet"
s3_filepath = "arrow-twitter/2022_01_parquet"



pf = pq.ParquetDataset(
    s3_filepath,
    filesystem=fs)

df = pf.read()

c = df.group_by("COMUNA").aggregate([("ESCOLARIDAD", "sum")])