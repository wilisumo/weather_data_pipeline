from src.common.common_helper import (
    LOGGER,
)
import json
import requests
import datetime
import calendar
import pandas as pd
from datetime import datetime, timedelta
import os
from decimal import Decimal
from boto3.dynamodb.types import TypeSerializer


class Ingestion:
    def __init__(self):
        self.serializer = TypeSerializer()

    def process(self, days: int):
        try:
            lst_loc = self.read_json(os.environ["LOCATIONS"])
            API_KEY = os.environ["API_KEY"]
            lst_response = []
            for dic in lst_loc:
                lat = dic.get("Latitude")
                lon = dic.get("Longitude")
                for day in range(0, int(days)):
                    d = datetime.utcnow() - timedelta(days=day)
                    time = calendar.timegm(d.utctimetuple())
                    response = requests.get(
                        f"{os.environ['END_POINT']}{lat}&lon={lon}&dt={time}&appid={API_KEY}"
                    )
                    json_response = response.json()
                    lst_response.append(json_response)
            LOGGER.info(lst_response)
            new_weather_df = self.transform(lst_response)
            output_df = self.clean_duplicates(new_weather_df)
            LOGGER.info(output_df.head())
            record_list = []
            for index, row in output_df.iterrows():
                dic = json.loads(row.to_json(), parse_float=Decimal)
                record = {k: self.serializer.serialize(v) for k, v in dic.items()}
                LOGGER.info(record)
                record_list.append(record)

            return record_list, output_df

        except Exception as e:
            LOGGER.info(f"Exception in process {e}")

    def read_json(self, json_path) -> list:
        lst_loc = []
        with open(json_path) as json_file:
            locations = json.load(json_file)
        for outer_k, outer_v in locations.items():
            for inner_k, inner_v in outer_v.items():
                lst_loc.append(inner_v)
        return lst_loc

    def transform(self, lst_response):
        lst_df = []
        for json_response in lst_response:
            hourly_json = json_response.get("hourly")
            df_weather = pd.DataFrame(hourly_json)
            df_weather["lat"] = json_response.get("lat")
            df_weather["lon"] = json_response.get("lon")
            df_weather["timezoneloc"] = str(json_response.get("timezone"))
            lst_df.append(df_weather)
            df = pd.concat(lst_df).reset_index()
            df.drop("index", axis=1, inplace=True)

        return df

    def flatten_json(self, nested_json, column_name, exclude=[""]):
        out = {}

        def flatten(x, name=column_name, exclude=exclude):
            if type(x) is dict:
                for a in x:
                    if a not in exclude:
                        flatten(x[a], name + a + "_")
            elif type(x) is list:
                i = 0
                for a in x:
                    flatten(a, name + "_")
                    i += 1
            else:
                out[name[:-1]] = x

        flatten(nested_json)
        return out

    def clean_duplicates(self, df):
        # columns = list(set(df.columns) - set(['weather','rain','snow']))
        columns = list(set(df.select_dtypes(["object"]).columns) - set(["timezoneloc"]))
        LOGGER.info(df.columns)
        df["timezoneloc"] = str(df["timezoneloc"])
        new_df = self.combine_duplicates(df=df, set_columns=columns)
        new_df["locationtime"] = new_df.dt.apply(
            lambda x: datetime.utcfromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S")
        )
        new_df["month"] = new_df.dt.apply(
            lambda x: datetime.utcfromtimestamp(x).strftime("%B")
        )
        new_df.drop_duplicates(keep="first", inplace=True)
        return new_df

    def combine_duplicates(self, df, set_columns):
        for value in set_columns:
            df = pd.concat(
                [
                    df.drop([value], axis=1),
                    pd.DataFrame(
                        [
                            self.flatten_json(nested_json=x, column_name=value)
                            for x in df[value]
                        ]
                    ),
                ],
                axis=1,
            )
        return df
