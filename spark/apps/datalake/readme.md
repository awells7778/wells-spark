# DATALAKE #
- Directory housing spark jobs for building our datalake.
- Our datalake is built via loading parquet files into an AWS S3 bucket
- Data is sourced from various API calls in the form of JSON, then transformed into parquet by spark, then loaded into target destination