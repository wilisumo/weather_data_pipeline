# -*- coding: utf-8 -*-
"""
Some common helper function
"""
import os
import sys
import configparser
import datetime
from dateutil.relativedelta import relativedelta
import logging
import boto3
import boto3.session
import pandas as pd
import numpy as np
import pickle
import s3fs
import pyarrow.parquet as pq


LOGGER = logging.getLogger()
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(asctime)s: %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
    handlers=[logging.StreamHandler()],
)


def init_env(env: str) -> None:
    """
    Set environment variables according to the current env(dev, staging or prod)
    :param env: The current running environment (dev, staging, prod)
    """

    config = configparser.ConfigParser()
    config.sections()
    config_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "../..", "config.ini"
    )
    config.read(config_path)
    if "dev" in env:
        LOGGER.info("INGRESO A DEV")
        os.environ["LOCALSTACK_ENDPOINT_URL"] = "http://localhost:4566"
        os.environ["AWS_ACCESS_KEY_ID"] = "foo"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "foo"
    os.environ["env"] = env
    os.environ["QUERY_MAX_TEMP_LOC"] = config["sql"]["query_max_temp_by_location"]
    os.environ["QUERY_STATS_DAY"] = config["sql"]["query_stats_by_by_day"]
    os.environ["LOCATIONS"] = config["json"]["locations"]
    os.environ["TABLE_LANDING"] = config[env]["table_landing"]
    os.environ["REFINED"] = config[env]["refined_bucket"]
    os.environ["ATTRIBUTES"] = config["json"]["json_attrs"]
    os.environ["KEYSCHEMA"] = config["json"]["json_keyschema"]
    os.environ["PROVISION"] = config["json"]["json_provision"]
    os.environ["API_KEY"] = config[env]["API_KEY"]
    os.environ["END_POINT"] = config[env]["end_point"]
    os.environ["PATH_01"] = config[env]["path_results_q1"]
    os.environ["PATH_02"] = config[env]["path_results_q2"]


def get_last_month():
    """
    Get the string of last month
    :return: the string of last month as format yyyy/mm
    """
    today = datetime.date.today()
    first = today.replace(day=1)
    lastMonth = first - datetime.timedelta(days=1)
    return lastMonth.strftime("%Y/%m")


def get_today():
    """
    Get the string of today
    :return: the string of today as format yyyy/mm/dd
    """
    today = datetime.date.today()
    return today.strftime("%Y/%m/%d")


def find_s3_full_path(s3_partial_url: str) -> str:
    """
    Return the full path of the S3 file.
    :param s3_partial_url: The S3 path contains partial matching regex (*)
    :return: The full S3 path
    """
    # The URL is a full one if there is no *
    if "*" not in s3_partial_url:
        return s3_partial_url

    bucket_name = s3_partial_url.split("/")[2]

    # Find prefix
    prefixs = s3_partial_url.split("*")[0].split("/")
    prefixs = prefixs[3 : len(prefixs) - 1]
    prefix = "/".join(prefixs)

    # Find UUID
    uuid_str = ""
    for path in s3_partial_url.split("/"):
        if "*-" in path:
            uuid_str = path[2:]

    # Find file name
    file_name = s3_partial_url.split("/")[-1]

    # Find full path
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=prefix):
        if uuid_str in obj.key and file_name in obj.key:
            return "s3://{0}/{1}".format(bucket_name, obj.key)
    return ""


def read_s3(s3_url: str):
    return find_s3_full_path(s3_url)


def write_s3(s3_url: str):
    return find_s3_full_path(s3_url)


def _key_existing_size__list(client, bucket, key):
    """return the key's size if it exist, else None"""
    response = client.list_objects_v2(Bucket=bucket, Prefix=key)
    for obj in response.get("Contents", []):
        if obj["Key"] == key:
            return obj["Size"]


def _key_files__list(client, bucket, key):
    """return the key's size if it exist, else None"""
    response = client.list_objects_v2(Bucket=bucket, Prefix=key)
    return response.get("Contents", [])


def file_exists(s3_url):
    session = boto3.session.Session()
    s3 = session.client("s3")
    bucket = s3_url.split("/")[2]
    key = s3_url.split("/")[3:]
    key = "/".join(key)
    size = _key_existing_size__list(s3, bucket, key)
    if size is not None:
        print(f"File exists: {s3_url}")
        return True
    return False


def parquet_exists(s3_url):
    session = boto3.session.Session()
    s3 = session.client("s3")
    bucket = s3_url.split("/")[2]
    key = s3_url.split("/")[3:]
    key = "/".join(key) + "/_SUCCESS"
    size = _key_existing_size__list(s3, bucket, key)
    if size is not None:
        print(f"File exists: {s3_url}")
        return True
    return False


def get_s3_files(s3_url):
    session = boto3.session.Session()
    s3 = session.client("s3")
    bucket = s3_url.split("/")[2]
    key = s3_url.split("/")[3:]
    key = "/".join(key)
    objects = _key_files__list(s3, bucket, key)
    files = ["s3://" + bucket + "/" + file["Key"] for file in objects]
    return files


def get_s3_parquets(bucket, key):
    session = boto3.session.Session()
    s3 = session.client("s3")
    # bucket = s3_url.split("/")[2]
    # key = s3_url.split("/")[3:]
    # key = "/".join(key)
    objects = _key_files__list(s3, bucket, key)
    files = [
        "s3://" + bucket + "/" + file["Key"][:-9]
        for file in objects
        if file["Key"].split("/")[-1] == "_SUCCESS"
    ]
    return files


def s3resource():
    s3resource = boto3.resource(
        "s3",
        region_name="us-east-1",
        endpoint_url=os.environ["LOCALSTACK_ENDPOINT_URL"],
    )
    return s3resource


def s3client():
    s3client = boto3.client("s3")
    return s3client


def remove_empty_from_dict(item):
    if type(item) is dict:
        return dict(
            (k, remove_empty_from_dict(v))
            for k, v in item.items()
            if v and remove_empty_from_dict(v)
        )
    elif type(item) is list:
        return [
            remove_empty_from_dict(v) for v in item if v and remove_empty_from_dict(v)
        ]
    else:
        return item


def get_dynamo_instance():

    if "dev" in sys.argv[3]:
        LOGGER.info(f"endpoint_url={os.environ['LOCALSTACK_ENDPOINT_URL']}")
        return boto3.client(
            "dynamodb",
            region_name="us-east-1",
            endpoint_url=os.environ["LOCALSTACK_ENDPOINT_URL"],
        )
    else:
        return boto3.client("dynamodb", region_name="us-east-1")
