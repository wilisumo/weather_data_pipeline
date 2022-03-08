import os
import configparser
import datetime as dt
from typing import Dict, List
from src.common.common_helper import (
    get_s3_parquets,
    rango_mapping,
    path_generator,
    LOGGER,
)
from src.common.AWSDynamo import AWSDynamodb

PRE_MDT_FILE_NAME = "pre_mdt_{0}"
MDT_FILE_NAME = "mdt_{0}"
MODEL_FILE_NAME = "model_{0}.pkl"
SCORE_FILE_NAME = "score_{0}.csv"

data_source_mapping = {
    "activo": "productos activo sarc M",
    "buro": "riesgos-creditos buro-comportamiento-financiero experian M",
    "moras": "cobranzas moras-ics sarc D",
}

data_mdt_mapping = {
    "mdt_pobla": "poblacion",
    "mdt_activo_tot": "activos_tot",
    "mdt_activo_tot_t": "activos_tot_t",
    "mdt_mora_tot": "moras_tot",
    "mdt_expr": "mdt_expr",
    "mdt_trad": "mdt_trad",
}

data_pref = {
    "activo": "estandarizado/productos/activo/productos_activo_sarc_M",
    "moras": "estandarizado/cobranzas/moras-ics/cobranzas_moras-ics_sarc_D",
    "buro": "estandarizado/riesgos-creditos/buro-comportamiento-financiero/riesgos-creditos_buro-comportamiento-financiero_experian_M",
}


def search_files_from_dynamo(bucket: str, data_source_name: str, period: str) -> str:
    """
    Search the S3 file path from Dynamo
    :param bucket: the name of bucket
    :param data_source_name: the name of data source which read from config.ini
    :param period: the period of data to be searched. e.g: 2019/06
    :return: the S3 file path
    """
    storage = AWSDynamodb(os.environ["TABLE_NAME_STD"])
    key_value = {
        "bucket_name": bucket,
        "path": path_generator(data_source_mapping.get(data_source_name), period),
    }
    LOGGER.info("Searching file {0} ...".format(key_value))
    path = storage.get(key_value)
    return path["Item"]["path"]


def search_files_from_mdt(bucket: str, data_source_name: str, period: str) -> str:
    paths = {
        "mdt_pobla": "poblacion/pre_mdt_poblacion_D{0}",
        "mdt_activo_tot": "activos_s/pre_mdt_activos_D{0}",
        "mdt_activo_tot_t": "activos_s/pre_mdt_activos_t_D{0}",
        "mdt_mora_tot": "moras/pre_mdt_moras_D{0}",
        "mdt_expr": "pre_mdt_expr/pre_mdt_expr_D{0}",
        "mdt_trad": "pre_mdt_trad/pre_mdt_trad_D{0}",
    }
    S3_PATH = "/".join(os.environ["PMDT"].split("/")[1:])
    return S3_PATH + "/" + paths.get(data_source_name).format(period)


def read_config() -> Dict:
    """
    Parse the config.ini file to find the data source name and relative delta
    :return:
    """
    config = configparser.ConfigParser()
    config.sections()
    config_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "../..", "config.ini"
    )
    config.read(config_path)
    period_source_dict = {}
    for key, val in config.items("sources_periods"):
        period_source_dict[key] = val

    return period_source_dict


def get_paths(period: str, start_period: str, end_period: str) -> Dict:
    dict_paths = {}
    missing_file = []
    if period:
        start_period = end_period = period
    if start_period is None or end_period is None:
        return {}
    else:
        start_period = dt.datetime.strptime(start_period, "%Y-%m-%dT%H:%M:%SZ")
        end_period = dt.datetime.strptime(end_period, "%Y-%m-%dT%H:%M:%SZ")
    period_source_dict = read_config()
    for source_name, deltas in period_source_dict.items():
        fechas = []
        rango = rango_mapping(deltas, start_period, end_period)
        if "M" in deltas:
            fechas = [x.strftime("%Y%m") for x in rango]
        elif "D" in deltas:
            fechas = [x.strftime("%Y%m%d") for x in rango]
        elif "L" in deltas:
            bucket = os.environ["STD_DATA"].split("/")[0]
            fechas_tot = [
                x[-6:] for x in get_s3_parquets(bucket, data_pref[source_name])
            ]
            fechas1 = [
                x
                for x in fechas_tot
                if (int(start_period.strftime("%Y%m")) <= int(x))
                & (int(x) < int(end_period.strftime("%Y%m")))
            ]
            fechas1 = sorted(fechas1, reverse=True)
            fechas2 = [
                x for x in fechas_tot if int(x) < int(start_period.strftime("%Y%m"))
            ]
            fechas2 = sorted(fechas2[int(deltas[:-1]) :], reverse=True)
            fechas = sorted(fechas1 + fechas2, reverse=True)

            del fechas1, fechas2, fechas_tot

        for data_period in fechas:
            bucket = os.environ["STD_DATA"].split("/")[0]
            path = ""
            try:
                path = search_files_from_dynamo(bucket, source_name, data_period)
            except Exception:
                pass
            if "" == path:
                missing_path = {
                    "bucket_name": bucket,
                    "path": path_generator(
                        data_source_mapping.get(source_name), data_period
                    ),
                }
                print(f"missing file: {missing_path}")
                LOGGER.info(f"missing file: {missing_path}")
                missing_file.append(path.split("/")[-1])
            elif source_name in dict_paths:
                files = dict_paths[source_name]
                files.append("s3://{}/{}".format(bucket, path.lstrip("/")))
                dict_paths[source_name] = files
            else:
                dict_paths[source_name] = [
                    "s3://{}/{}".format(bucket, path.lstrip("/"))
                ]

    if len(missing_file) > 2:
        LOGGER.info("Failed - Missing File...")
        print("Failed - Missing File...")

        return False

    return dict_paths


def get_paths_mdt(period, start_period: str, end_period: str) -> List:
    bucket = os.environ["PMDT"].split("/")[0]
    dict_paths = {}
    for key, source_name in data_mdt_mapping.items():
        path = search_files_from_mdt(bucket, key, period)
        if "" == path:
            return {}
        dict_paths[key] = "s3://{}/{}".format(bucket, path.lstrip("/"))

    return dict_paths
