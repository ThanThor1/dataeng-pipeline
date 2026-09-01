# docker/spark/Dockerfile
FROM apache/spark:3.5.1-python3

USER root

# Thư mục chứa JAR connector (Kafka + S3A) trong image chính thức Apache
ENV SPARK_JARS_DIR=/opt/spark/jars

# Tải các JAR cần thiết để Spark nói chuyện được với Kafka và MinIO (S3A)
RUN cd ${SPARK_JARS_DIR} && \
    curl -fSL -o spark-sql-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -fSL -o spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -fSL -o kafka-clients-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -fSL -o commons-pool2-2.11.1.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -fSL -o hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fSL -o aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Thư viện Python cần cho code Spark job
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Thư mục sẽ mount code job Python từ host vào
RUN mkdir -p /opt/spark_apps && chown -R spark:spark /opt/spark_apps

USER spark
WORKDIR /opt/spark
