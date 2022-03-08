from src.common.common_helper import (
    LOGGER,
)
import pandasql as ps
import awswrangler as wr
import os


class Analize:
    # def __init__(self):

    def process(self, df, sql_path: str):
        try:
            LOGGER.info("Analizing Status Running...")
            LOGGER.info(df.head())
            LOGGER.info(sql_path)
            with open(sql_path) as file_sql:
                sql_content = file_sql.read().replace("\n", "")
            df_result = ps.sqldf(sql_content)
            LOGGER.info("FINISH ANALIZING...")

            return df_result
        except Exception as e:
            LOGGER.info(f"Exception in Analize {e}")
            LOGGER.error(f"Exception exporting file {e} {path}")

    def export(self, s3, df, path, partition_cols):
        try:
            wr.config.s3_endpoint_url = os.environ["LOCALSTACK_ENDPOINT_URL"]
            wr.s3.to_parquet(
                df=df,
                path=path,
                dataset=True,
                mode="overwrite",
                partition_cols=partition_cols,
            )
        except Exception as e:
            LOGGER.info(f"Exception exporting file {e} {path}")
            LOGGER.error(f"Exception exporting file {e} {path}")
