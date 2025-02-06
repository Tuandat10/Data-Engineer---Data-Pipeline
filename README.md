This project demonstrates a data pipeline using Apache Kafka, Apache Spark, and Apache Airflow to process and analyze streaming data. The pipeline reads data from a mock API, processes it using Spark, and writes the results to a MySQL database

## How It Works
### Components
Kafka: Acts as the message broker for streaming data.
Spark: Processes the streaming data.
Airflow: Orchestrates the data pipeline.
MySQL: Stores the processed data.
## Workflow
### Data Ingestion:

The kafka_stream.py script fetches data from a mock API and sends it to a Kafka topic named test1.
Result: The raw data from the API is now available in the Kafka topic test1.
### Data Processing:

The stream_from_kafka.py script reads the data from the Kafka topic, processes it using Spark, and writes the results to a MySQL database.
Result: The data is transformed and cleaned, then stored in the MySQL database for further analysis.
### Orchestration:

The airflow_automation.py script defines an Airflow DAG that orchestrates the data pipeline. It uses DockerOperator to run the Spark job inside a Docker container.
Result: The entire data pipeline is automated and can be scheduled to run at specific intervals or triggered manually.

## Detailed Steps
### Data Ingestion:

The kafka_stream.py script connects to a mock API and retrieves a batch of data.
This data is then serialized and sent to the Kafka topic test1.
Kafka acts as a buffer, allowing the data to be consumed by multiple consumers independently.
### Data Processing:

The stream_from_kafka.py script sets up a Spark session configured to read from Kafka.
It reads the data from the Kafka topic test1 as a streaming DataFrame.
The data is then processed using Spark transformations, such as filtering, mapping, and aggregations.
The processed data is written to a MySQL database, where it is stored in a structured format.
### Orchestration:

The airflow_automation.py script defines an Airflow DAG that includes tasks for each step of the pipeline.
The DAG uses DockerOperator to run the Spark job inside a Docker container, ensuring a consistent environment.
Airflow schedules and monitors the execution of the pipeline, providing a web interface to track the status and logs of each task.


## Results
The processed data is stored in the MySQL database specified in the docker-compose.yml file.
You can view the processed data by connecting to the MySQL database and querying the relevant tables.
The data is now ready for further analysis, reporting, or machine learning tasks.
## Running the Project
### Setup:
git clone Tuandat10/Data-Engineer---Data-Pipeline/
cd to the folder contains docker-compose.yml

Ensure Docker and Docker Compose are installed on your machine.
Clone the repository and navigate to the project directory.
### Start the Services:

docker-compose up -d
Use Docker Compose to start the services defined in the docker-compose.yml file.
### Trigger the Airflow DAG:

Access the Airflow web interface at http://localhost:8080 and trigger the run_kafka_stream DAG.
### Monitor the Pipeline:

Monitor the logs and status of the pipeline using the Airflow web interface.

## Conclusion
This project demonstrates a complete data pipeline using Kafka, Spark, and Airflow. It showcases how to ingest, process, and store streaming data in a scalable and efficient manner.
