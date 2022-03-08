# Project Title

Data engineering test

## Description

project that extract information from a weather API and process and analize it

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

$DAYS = The number of days to be ingested for weather API max 5
$JOB_ID= the name of the job for Idempotency purpose 
$ENV= the name of the environment (dev)
$JOB_NAME= the name of the process to be executed (creation_job, ingestion_process_job)

```
python3 main.py --days=$DAYS --aws_job_id=$JOB_ID --env=$ENV --job=$JOB_NAME
```

## Authors

William Suarez
wilisumo@gmail.com
## Version History

* 0.1
    * Initial Release

