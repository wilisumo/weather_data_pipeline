# -*- coding: utf-8 -*-

import os
import datetime
from typing import Dict, List

from src.common.common_helper import LOGGER

# from src.common.AWSDynamo import AWSDynamodb


class Lineage:
    status_code = "000"
    timelist = []

    def __init__(self, start, end, process):
        self.dynamo = AWSDynamodb(self.gettabledynamo(process))
        self.generation_date = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        self.start_process = start
        self.end_process = end

    def process_table_integration(self, job, status_code, message_error):
        lineagejson = self.create_dynamo_integration(job, status_code, message_error)
        self.dynamo.save(lineagejson)
        return lineagejson

    def process_table_process(self, job, subprocess, path_in, path_out, nrows):
        lineagejson = self.create_dynamo_process(
            job, subprocess, path_in, path_out, nrows
        )
        self.dynamo.save(lineagejson)
        return lineagejson

    def create_dynamo_integration(self, job, status_code, message_error):
        json_dynamo = self.get_json_save_dynamo_integration()
        json_dynamo["p_duration"] = self.get_duration_process()
        json_dynamo["job"] = job
        json_dynamo["p_status"] = self.status_process(status_code)
        json_dynamo["p_start"] = self.start_process
        json_dynamo["p_end"] = self.end_process
        json_dynamo["p_message_error"] = message_error
        return json_dynamo

    def create_dynamo_process(self, job, subprocess, path_in, path_out, nrows):
        json_dynamo = self.get_json_save_dynamo_process()
        json_dynamo["job"] = job
        json_dynamo["process"] = subprocess
        json_dynamo["p_path_in"] = path_in
        json_dynamo["p_path_out"] = path_out
        json_dynamo["p_rows"] = str(nrows)
        json_dynamo["p_filename_out"] = path_out.split("/")[-1]
        return json_dynamo

    def get_json_save_dynamo_integration(self) -> Dict:
        json = {
            "cu_name": os.environ["USE_CASE"],
            "job": None,
            "p_generation_date": self.generation_date,
            "p_start": None,
            "p_end": None,
            "p_duration": None,
            "p_status": None,
            "p_message_error": None,
        }
        return json

    def get_json_save_dynamo_process(self) -> Dict:
        json = {
            "job": None,
            "process": None,
            "p_path_in": None,
            "p_path_out": None,
            "p_rows": None,
            "p_status": None,
        }
        return json

    def endtoend(self, job_endtoend: str, job: str, status_code: str):
        json_dynamo = self.get_json_save_dynamo_integration()
        json_dynamo["p_status"] = self.status_process(status_code)
        if status_code == "100":
            key_value = {"cu_name": os.environ["USE_CASE"], "job": job_endtoend}
            self.dynamo.attributevalueupdate(
                key_value, "p_status", json_dynamo["p_status"]
            )
        else:
            key_value = {"cu_name": os.environ["USE_CASE"], "job": job}
            jobtemp = self.dynamo.get(key_value)
            self.timelist.append(jobtemp["Item"]["p_duration"])

            key_value = {"cu_name": os.environ["USE_CASE"], "job": job_endtoend}
            jobete = self.dynamo.get(key_value)
            self.timelist.append(jobete["Item"]["p_duration"])

            self.start_process = self.checkstart(
                jobtemp["Item"]["p_start"], jobete["Item"]["p_start"]
            )
            self.end_process = jobtemp["Item"]["p_end"]
            duration = self.get_duration_process()

            json_dynamo["job"] = job_endtoend
            json_dynamo["p_duration"] = duration
            json_dynamo["p_start"] = self.checkstart(
                jobtemp["Item"]["p_start"], jobete["Item"]["p_start"]
            )
            json_dynamo["p_end"] = jobtemp["Item"]["p_end"]
            json_dynamo["p_message_error"] = jobtemp["Item"]["p_message_error"]
            json_dynamo["p_path_in"] = jobete["Item"]["p_path_in"]
            self.dynamo.save(json_dynamo)

    def get_duration_process(self):
        if self.end_process:
            total_time = datetime.datetime.strptime(
                self.end_process, "%H:%M:%S"
            ) - datetime.datetime.strptime(self.start_process, "%H:%M:%S")
            return str(total_time)

    def updateinputpath(self, key_value, field, process, newvalue):
        temp = self.dynamo.get(key_value)
        if "p_path_in" in temp["Item"]:
            temp["Item"]["p_path_in"].update({process: newvalue})
            newvalue = temp["Item"]["p_path_in"]
        else:
            newvalue = {process: newvalue}
        self.dynamo.attributevalueupdate(key_value, field, newvalue)

    @staticmethod
    def status_process(status_code):
        value_status = {
            "000": "PENDING",
            "100": "RUNNING",
            "200": "SUCCEEDED",
            "400": "FAILED",
        }
        return value_status.get(status_code)

    @staticmethod
    def checkstart(start: str, start_e2e: str):
        """
        chequea si para el job end2end existe informacion en el campo "start".
        Este campo solo se debe actualizar en el 1 job ejecutado
        """
        if start_e2e == "00:00:00":
            return start
        else:
            return start_e2e

    @staticmethod
    def gettabledynamo(process: str):
        tables = {
            "1": os.environ["TABLE_NAME_INTEGRATION"],
            "2": os.environ["TABLE_NAME_PROCESS"],
            "3": os.environ["TABLE_NAME_QA"],
        }
        return tables.get(process)
