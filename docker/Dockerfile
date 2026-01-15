FROM openjdk:11
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN apt-get update && apt-get install -y python3 python3-pip \
    && pip3 install pyspark pandas delta-spark openpyxl apache-airflow

WORKDIR /app
COPY . .

CMD ["spark-submit", "scripts/generate_gold_delta_cdf.py"]

