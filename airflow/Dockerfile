FROM docker:28.5.2-cli AS docker-cli

FROM apache/airflow:3.3.0-python3.11

USER root
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker

COPY airflow/requirements.txt /tmp/project-requirements.txt

USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow==3.3.0" \
    -r /tmp/project-requirements.txt
