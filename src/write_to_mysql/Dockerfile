FROM bitnami/spark:3.5.0

WORKDIR /opt/bitnami/spark
USER root
ENV PYSPARK_MAJOR_PYTHON_VERSION=3
RUN apt-get update
RUN apt install -y python3 python3-pip

COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip3 install -r requirements.txt --user

COPY ./stream_from_kafka.py ./stream_from_kafka.py
COPY ./transform.py ./transform.py