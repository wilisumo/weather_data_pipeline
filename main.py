import click
import os
import time
from decimal import Decimal

# import watchtower
import datetime
from src.common.common_helper import init_env, LOGGER, s3resource
from src.common.Lineage import Lineage
from src.creation.Creation import Creation
from src.ingestion.Ingestion import Ingestion
import json
from src.store.Store import Store
from src.analize.Analize import Analize


def run_job_creation(aws_job_id: str, days) -> None:
    """
    run the job ingestion process that access data from a weather API,
    insert it into a DynamoDB Database, and generate some queries
    over the data that are exported to a S3 bucket.

    aws_job_id: the job id to be executed
    days: the number of days of the data to be ingested from the weather API

    """
    try:
        LOGGER.info("Creation Status Running")
        start_time = datetime.datetime.now().time().strftime("%H:%M:%S")
        with open(os.environ["ATTRIBUTES"]) as json_attr:
            attr = json.load(json_attr)
        with open(os.environ["KEYSCHEMA"]) as json_key_schema:
            key_schema = json.load(json_key_schema)
        with open(os.environ["PROVISION"]) as json_provision:
            provisions = json.load(json_provision)
        create_table = Creation()

        create_table.process(
            table_name=os.environ["TABLE_LANDING"],
            attributes=attr,
            schema=key_schema,
            provisions=provisions,
        )

        end_time = datetime.datetime.now().time().strftime("%H:%M:%S")
        LOGGER.info(f"CREATION FINISH SUCCESSFULLY AT {end_time}")

    except Exception as e:
        LOGGER.info("Ingestion status Failed")
        LOGGER.info(f"{e}")
        end_time = datetime.datetime.now().time().strftime("%H:%M:%S")


def split_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def run_job_ingestion_process(aws_job_id: str, days) -> None:
    """
    run the job ingestion process that access data from a weather API,
    insert it into a DynamoDB Database, and generate some queries
    over the data that are exported to a S3 bucket.

    aws_job_id: the job id to be executed
    days: the number of days of the data to be ingested from the weather API

    """
    try:
        LOGGER.info("Ingestion Status Running")
        start_time = datetime.datetime.now().time().strftime("%H:%M:%S")
        ingestion = Ingestion()
        record_list, output_df = ingestion.process(days)
        chunk_list = list(split_list(record_list, 25))
        store_item = Store()
        for i, chunk in enumerate(chunk_list):
            store_item.store_transaction(
                item=chunk,
                job_id=aws_job_id + "_" + str(i),
                PK="timezoneloc",
                table=os.environ["TABLE_LANDING"],
            )
        analize = Analize()
        path = f"s3://{os.environ['REFINED']+'/'+os.environ['PATH_01']+'/'}"
        result_query = analize.process(
            df=output_df, sql_path=str(os.environ["QUERY_MAX_TEMP_LOC"])
        )
        analize.export(
            df=result_query, s3=s3resource, path=path, partition_cols=["date"]
        )
        result_query_02 = analize.process(
            df=output_df, sql_path=str(os.environ["QUERY_STATS_DAY"])
        )
        path = f"s3://{os.environ['REFINED']+'/'+os.environ['PATH_02']+'/'}"
        analize.export(
            df=result_query_02,
            s3=s3resource,
            path=path,
            partition_cols=["locationtime"],
        )
        end_time = datetime.datetime.now().time().strftime("%H:%M:%S")
        LOGGER.info(f"PROCESS END TO END FINISH SUCCESSFULLY AT {end_time}")

    except Exception as e:
        LOGGER.info("END TO END status Failed")
        LOGGER.error(f"END TO END status Failed {e}")
        LOGGER.info(f"{e}")
        end_time = datetime.datetime.now().time().strftime("%H:%M:%S")


@click.command()
@click.option("--overwrite", help="True, False", required=False)
@click.option("--days", help="int value from 1 to n", required=True)
@click.option("--aws_job_id", help="The current Job ID")
@click.option("--job", help="creation_job, ingestion_process_job", required=True)
# @click.option("--job", help="Job name ")


@click.option(
    "--env",
    default="prod",
    help="'dev' for development, 'prod' for production environment",
)
def main(job, overwrite, days, env, aws_job_id):
    start = time.time()
    # os.environ["OVERWRITE_DATA"] = overwrite or "False"
    days = days or 5
    init_env(env)
    # Add Handler to cloudwatch logs
    # LOGGER.addHandler(
    #    watchtower.CloudWatchLogHandler(log_group=os.environ["CLOUDWATCH_LOG_GROUP"])
    # )
    LOGGER.info(f"Job: {job} {aws_job_id}")
    LOGGER.info(f"Overwrite files: {overwrite}")
    LOGGER.info("Execution Days: {0}".format(days))
    jobname = "-".join([env, days, aws_job_id])
    os.environ["AWS_BATCH_JOB_ID"] = aws_job_id
    LOGGER.info(f"Starting a job: {jobname}")
    # Start a job
    function_dict = {
        "creation_job": run_job_creation,
        "ingestion_process_job": run_job_ingestion_process,
        # "processing_job": run_job_processing,
        # "analize_job": run_job_analyzing,
    }
    # job_function = function_dict.get("creation_job")
    # job_function(aws_job_id, days)
    job_function = function_dict.get(job)
    job_function(aws_job_id, days)


if __name__ == "__main__":
    # allows to set the aws access key
    main(auto_envvar_prefix="X")
