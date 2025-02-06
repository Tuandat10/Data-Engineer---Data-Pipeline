FROM apache/airflow:2.10.0-python3.11

WORKDIR /app

COPY requirements_stream.txt /app/

RUN pip install --no-cache-dir -r requirements_stream.txt
ENV PYTHONPATH="/app/src:${PYTHONPATH}"

COPY . /app/
COPY pipeline_project.json /app/

CMD ["bash", "-c", "airflow db upgrade && airflow scheduler & airflow webserver"]