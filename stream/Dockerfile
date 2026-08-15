FROM python:3.11-slim

WORKDIR /opt/airflow

RUN pip install --no-cache-dir boto3==1.34.* psycopg2-binary==2.9.* pydantic==2.*

COPY common /opt/airflow/common
COPY stream /opt/airflow/stream

ENV PYTHONPATH=/opt/airflow/common:/opt/airflow

CMD ["python", "/opt/airflow/stream/consumer.py"]
