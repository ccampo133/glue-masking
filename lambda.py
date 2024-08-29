import boto3
import aws

GLUE_JOB_NAME = "masking-job"
SRC_DB = "srcdb"
DEST_DB = "destdb"

glue = boto3.client("glue")


# get_glue_tables returns the list of tables in the specified AWS Glue data
# catalog database using the Glue API.
def get_glue_tables(database=None):
    next_token = ""
    tables = []
    while True:
        response = glue.get_tables(DatabaseName=database, NextToken=next_token)
        for table in response.get("TableList"):
            tables.append(table.get("Name"))
        next_token = response.get("NextToken")
        if next_token is None:
            break
    return tables


def lambda_handler(event, context):
    tables = aws.get_glue_tables(database=SRC_DB)
    print(f"Got {len(tables)} tables for {SRC_DB} from Glue data catalog")
    for table in tables:
        print(f"Starting Glue job {GLUE_JOB_NAME} for table: {table} (from {SRC_DB} to {DEST_DB})")
        glue.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--src_db": SRC_DB,
                "--dest_db": DEST_DB,
                "--table_name": table,
            },
        )
    return {
        "message": f"Started {len(tables)} Glue jobs ({GLUE_JOB_NAME}) tables in {SRC_DB} (to {DEST_DB})",
    }
