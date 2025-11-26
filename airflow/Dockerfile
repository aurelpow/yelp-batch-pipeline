FROM apache/airflow:3.1.0

USER root

# Install OpenJDK 17 and download Spark
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3

# Download and install Apache Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

USER airflow

# Install Apache Spark provider
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==4.10.0

# Copy JARs
#COPY --chown=airflow:root lib/postgresql-42.7.8.jar /opt/airflow/jars/
#COPY --chown=airflow:root target/scala-2.12/yelp-batch-project-assembly-0.1.0-SNAPSHOT.jar /opt/airflow/jars/


# Copy configuration files
#COPY --chown=airflow:root src/main/resources/*.conf /opt/airflow/config/

# Provide Spark defaults so spark-submit automatically loads Delta & MongoDB connector
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf