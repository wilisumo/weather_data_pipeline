# Project Title

Data engineering test

## Description

Extract the last 5 days of data from the free API: https://api.openweathermap.org/data/2.5/onecall/timemachine (Historical weather data) from 10 different locations to choose by the candidate.

Build a repository of data where we will keep the data extracted from the API. This repository should only have deduplicated data. Idempotency should also be guaranteed.

Build another repository of data that will contain the results of the following calculations from the data stored in step 2.

A dataset containing the location, date and temperature of the highest temperatures reported by location and month.
A dataset containing the average temperature, min temperature, location of min temperature, and location of max temperature per day.

## Getting Started

### Dependencies

*awswragler
*localstack
*boto3


## Executing program


To lauch docker container with local stack and execute the process use

```
docker compose-up
```

To execute the process manually
```
$DAYS = The number of days to be ingested for weather API max 5
$JOB_ID= the name of the job for Idempotency purpose 
$ENV= the name of the environment (dev)
$JOB_NAME= the name of the process to be executed (creation_job, ingestion_process_job)
```
```
python3 main.py --days=$DAYS --aws_job_id=$JOB_ID --env=$ENV --job=$JOB_NAME
```

## Authors

William Suarez
wilisumo@gmail.com
## Version History

* 0.1
    * Initial Release

