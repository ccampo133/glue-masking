import mask
import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Argument names.
JOB_NAME_ARG = "JOB_NAME"
SRC_DB_ARG = "src_db"
DEST_DB_ARG = "dest_db"
TABLE_NAME_ARG = "table_name"
# Constants
HASH_EXPRESSION = "hashexpression"
HASH_PARTITIONS = "hashpartitions"
# The number of partitions used for parallel JDBC reads. It should ideally be a
# multiple of the number of cores in the cluster. For this job, we"re using 10
# G.1X workers, which have 4 vCPUs each. One worker is the master, which leaves
# 9 workers. Therefore, the max number of partitions we can have is 4 * 9 = 36.
# See:
# * https://docs.aws.amazon.com/prescriptive-guidance/latest/tuning-aws-glue-for-apache-spark/parallelize-tasks.html
# * https://docs.aws.amazon.com/glue/latest/dg/run-jdbc-parallel-read-job.html
# * https://www.youtube.com/watch?v=qLkTe9kT0PU
# * https://www.youtube.com/watch?v=y-W2IHB8j4w
DEFAULT_HASH_PARTITIONS = 36

# Column names to mask for each table.
# TODO: this should be a parameter -ccampo 2024-08-07
mask_cols = {
    "postgres_public_customers": ["name", "address"],
    "postgres_public_orders": ["total"],
    "postgres_public_payments": ["credit_card"],
}

# AWS Glue boilerplate
args = getResolvedOptions(
    sys.argv,
    # Job parameters.
    [
        JOB_NAME_ARG,
        SRC_DB_ARG,
        DEST_DB_ARG,
        TABLE_NAME_ARG
    ]
)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args[JOB_NAME_ARG], args)

# Init job parameters.
src_db = args[SRC_DB_ARG]
dest_db = args[DEST_DB_ARG]
table_name = args[TABLE_NAME_ARG]

# Register the UDF. Here we're using the consistent_mask function from the mask
# module, although any masking function can be used.
consistent_mask_udf = udf(mask.consistent_mask, StringType())


# etl is an extract/transform/load (ETL) function that extracts data from a
# source table, transforms it by masking some specified data, and loads it into
# a destination table. It assumes that the source and destination tables have
# the same name and schema.
def etl(tbl):
    # Read data from source table (extract).
    print(f"Extracting data from src table: {tbl}")
    # Parallelize JDBC reads. See:
    # * https://docs.aws.amazon.com/prescriptive-guidance/latest/tuning-aws-glue-for-apache-spark/parallelize-tasks.html
    # * https://docs.aws.amazon.com/glue/latest/dg/run-jdbc-parallel-read-job.html
    # * https://www.youtube.com/watch?v=qLkTe9kT0PU
    # * https://www.youtube.com/watch?v=y-W2IHB8j4w
    #
    # In this example, every table has an "id" column as a primary key, so we
    # can use that as the hash expression. Note that in general, the hash
    # expression might be different for each table.
    opts = {HASH_EXPRESSION: "id", HASH_PARTITIONS: DEFAULT_HASH_PARTITIONS}
    src = glueContext.create_dynamic_frame.from_catalog(
        database=src_db,
        table_name=tbl,
        transformation_ctx=f"ds_{tbl}",
        additional_options=opts
    )
    # Apply the mask to the specified columns (transform).
    cols = mask_cols.get(tbl)
    if cols:
        df = src.toDF()
        for col_name in cols:
            print(f"Masking column: {tbl}.{col_name}")
            df = df.withColumn(col_name, consistent_mask_udf(col(col_name)))
        src = DynamicFrame.fromDF(df, glueContext, src.name)
    # Write to destination table (load).
    print(f"Loading (writing) data to dest table: {tbl}")
    glueContext.write_dynamic_frame.from_catalog(
        frame=src,
        database=dest_db,
        table_name=tbl,
        transformation_ctx=f"ds_{tbl}"
    )


# Start of the ETL job.
print(f"Starting ETL for table {table_name}")
etl(table_name)
print(f"ETL complete for {table_name}")

job.commit()
