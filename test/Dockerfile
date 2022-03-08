FROM python:3.7.4

ENV LANG=C.UTF-8 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    WAITFORIT_VERSION="v2.4.1" \
    IN_A_TEST_DOCKER_CONTAINER=True


WORKDIR /repo

RUN apt-get update \
    && apt-get install -y groff less curl \
    && pip install --upgrade pip \
    && curl -o /usr/local/bin/waitforit -sSL https://github.com/maxcnunes/waitforit/releases/download/$WAITFORIT_VERSION/waitforit-linux_amd64 \
    && chmod +x /usr/local/bin/waitforit

COPY requirements.txt ./

RUN pip install --no-cache-dir --requirement requirements.txt

COPY . ./


RUN python3 main.py --days=5 --aws_job_id="2021-03-07 22:02:25" --env="dev"

RUN python3 main.py --days=5 --aws_job_id="2021-03-07 22:02:25" --env="dev"


