import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, current_timestamp

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Set up Glue context and Spark session
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data from Glue Catalog
reddit_raw = glueContext.create_dynamic_frame.from_catalog(
    database="reddit_db",          
    table_name="raw_data"          
)

# Convert to DataFrame
df = reddit_raw.toDF()

# Add current timestamp column
df = df.withColumn("Date", current_timestamp())

# Select and clean necessary columns
cleaned_df = df.select(
    col("title"),
    col("url"),
    col("score").cast("int"),
    col("author"),
    col("Date")
).dropna()

# ✅ Remove duplicates based on key fields
cleaned_df = cleaned_df.dropDuplicates(["title", "url", "author"])

# Convert back to DynamicFrame
cleaned_dynamic_df = DynamicFrame.fromDF(cleaned_df, glueContext, "cleaned_dynamic_df")

# Write cleaned, deduplicated data to S3 in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=cleaned_dynamic_df,
    connection_type="s3",
    connection_options={"path": "s3://myredditbuckkk/clean_data/"},
    format="parquet"
)

# Commit the job
job.commit()
