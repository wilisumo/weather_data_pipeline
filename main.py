import click
import os
import time
from decimal import Decimal

# import watchtower
# from typing import Dict
import datetime

# from src.common import path_helper
from src.common.common_helper import init_env, LOGGER, s3resource
from src.common.Lineage import Lineage
from src.creation.Creation import Creation
from src.ingestion.Ingestion import Ingestion
import json
from src.store.Store import Store
from src.analize.Analize import Analize


def run_job_creation(aws_job_id: str, days) -> None:
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

    except Exception as e:
        LOGGER.info("Ingestion status Failed")
        LOGGER.info(f"{e}")
        end_time = datetime.datetime.now().time().strftime("%H:%M:%S")


def split_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def run_job_ingestion_process(aws_job_id: str, days) -> None:
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
        LOGGER.info(type(output_df))
        result_query = analize.process(
            df=output_df, sql_path=str(os.environ["QUERY_MAX_TEMP_LOC"])
        )
        LOGGER.info("result query")
        LOGGER.info(result_query.head())
        path = f"s3://{os.environ['REFINED']+'/'+os.environ['PATH_01']+'/'}"
        LOGGER.info(path)
        analize.export(
            df=result_query, s3=s3resource, path=path, partition_cols=["date"]
        )

        end_time = datetime.datetime.now().time().strftime("%H:%M:%S")

    except Exception as e:
        LOGGER.info("END TO END status Failed")
        LOGGER.error(f"END TO END status Failed {e}")
        LOGGER.info(f"{e}")
        end_time = datetime.datetime.now().time().strftime("%H:%M:%S")


def run_job_prep(
    start_period: str,
    end_period: str,
    period: str,
    sql_context,
    con_sin_acierta,
    aws_job_id,
) -> None:
    """
    Run the preparation job
    :param aws_job_id: the AWS Batch job ID
    """
    try:
        LOGGER.info("Lineage Status Running")
        start_time = datetime.datetime.now().time().strftime("%H:%M:%S")
        jobname = "-".join(["preparation", period, aws_job_id])
        jobnameetoe = "-".join(["endtoend", period, aws_job_id])
        subprocess = "-".join(["preparation", period])
        datalineage = Lineage(start_time, None, "1")
        datalineage.process_table_integration(jobname, "100", None)
        datalineage.endtoend(jobnameetoe, jobname, "100")
        LOGGER.info("Initializing preparation process...")
        # input_paths = path_helper.get_paths_dynamo_pmdt(period)
        input_paths = path_helper.get_paths(period, start_period, end_period)
        key_value = {"cu_name": os.environ["USE_CASE"], "job": jobnameetoe}
        datalineage.updateinputpath(key_value, "p_path_in", "preparation", input_paths)
        if not input_paths:
            LOGGER.info("Failed - Missing File...")
        else:
            df_pre_mdt, path_premdt = prep_process(
                input_paths, start_period, end_period, period, sql_context
            )
            LOGGER.info("PMDT created")
            output_path = save_pre_mdt(df_pre_mdt, path_premdt, sql_context)
            LOGGER.info("Lineage Status Suceeded")
            end_time = datetime.datetime.now().time().strftime("%H:%M:%S")
            datalineage = Lineage(start_time, end_time, "1")
            datalineage.process_table_integration(jobname, "200", "-")
            LOGGER.info("Inserting table integration finished")
            datalineage.endtoend(jobnameetoe, jobname, "200")
            datalineage = Lineage(start_time, end_time, "2")
            row_numbers = {}
            for key, value in df_pre_mdt.items():
                row_numbers[key] = value.shape[0]
            datalineage.process_table_process(
                jobname, subprocess, input_paths, output_path, row_numbers
            )
            LOGGER.info("Inserting table process finished")
            LOGGER.info(f"Job {jobnameetoe} {aws_job_id} finished")

    except Exception as e:
        LOGGER.info("Lineage status Failed")
        LOGGER.info(f"{e}")
        end_time = datetime.datetime.now().time().strftime("%H:%M:%S")
        datalineage = Lineage(start_time, end_time, "1")
        datalineage.process_table_integration(jobname, "400", f"{e}")
        datalineage.endtoend(jobnameetoe, jobname, "400")


def run_job_scoring(
    start_period: str,
    end_period: str,
    period: str,
    sql_context,
    con_sin_acierta,
    aws_job_id,
) -> dict:
    """
    Run the scoring job
    :param aws_job_id: the AWS Batch job ID
    """
    try:
        LOGGER.info("Lineage Status Running")
        start_time = datetime.datetime.now().time().strftime("%H:%M:%S")
        jobname = "-".join(["scoring", period, aws_job_id])
        jobnameetoe = "-".join(["endtoend", period, aws_job_id])
        subprocess = "-".join(["scoring", period])
        datalineage = Lineage(start_time, None, "1")
        datalineage.process_table_integration(jobname, "100", None)
        datalineage.endtoend(jobnameetoe, jobname, "100")
        LOGGER.info("Initializing scoring process...")
        period = datetime.datetime.strptime(period, "%Y-%m-%dT%H:%M:%SZ").strftime(
            "%Y%m%d"
        )
        input_paths = path_helper.get_paths_mdt(period, start_period, end_period)
        key_value = {"cu_name": os.environ["USE_CASE"], "job": jobnameetoe}
        datalineage.updateinputpath(key_value, "p_path_in", "scoring", input_paths)
        output_path, n_rows = scoring(
            input_paths, start_period, end_period, period, sql_context, con_sin_acierta
        )
        input_paths = {"mdt": [input_paths]}
        LOGGER.info("Lineage Status Suceeded")
        end_time = datetime.datetime.now().time().strftime("%H:%M:%S")
        datalineage = Lineage(start_time, end_time, "1")
        datalineage.process_table_integration(jobname, "200", "-")
        datalineage.endtoend(jobnameetoe, jobname, "200")
        datalineage = Lineage(start_time, end_time, "2")
        datalineage.process_table_process(
            jobname, subprocess, input_paths, output_path["score2"], n_rows
        )
        LOGGER.info("Scoring Suceeded")
        result_message = "finalizado correctamente, se generó el archivo: {}".format(
            "/".join(output_path["score2"].split("/")[-1:])
        )
        path_helper.notify_ses(
            msg=result_message, start_period=period, type_notify="successful"
        )
    except Exception as e:
        LOGGER.info("Lineage Status Failed")
        LOGGER.info(f"{e}")
        end_time = datetime.datetime.now().time().strftime("%H:%M:%S")
        datalineage = Lineage(start_time, end_time, "1")
        datalineage.process_table_integration(jobname, "400", f"{e}")

        datalineage.endtoend(jobnameetoe, jobname, "400")
        path_helper.notify_ses(
            msg="error de ejecución", start_period=period, type_notify="error"
        )


def run_job_backtesting(
    start_period: str, end_period: str, period: str, sql_context, con_sin_acierta
) -> dict:
    """
    Run the preparation job
    :param aws_job_id: the AWS Batch job ID
    """
    LOGGER.info("Starting Preparation")
    files = path_helper.get_paths(period, start_period, end_period)
    pre_mdt = back_prep_process(files, start_period, end_period, sql_context)
    LOGGER.info(pre_mdt)
    LOGGER.info("Starting Scoring")
    back_scoring(files, start_period, end_period, sql_context, con_sin_acierta)
    LOGGER.info("end scoring")


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
