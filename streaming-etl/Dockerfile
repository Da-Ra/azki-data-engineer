FROM bitnami/spark:3.5.1

USER root

RUN install_packages curl

# ClickHouse JDBC driver (the -all classifier bundles transitive deps;
# easier than asking spark-submit --packages to resolve it).
RUN curl -fSL -o /opt/bitnami/spark/jars/clickhouse-jdbc-0.6.0-all.jar \
    https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.0/clickhouse-jdbc-0.6.0-all.jar

# Ensure UID 1001 has a passwd entry so Hadoop's UserGroupInformation can resolve a name.
RUN id -u 1001 >/dev/null 2>&1 || echo "spark:x:1001:1001::/home/spark:/bin/bash" >> /etc/passwd

# Pre-create the checkpoint dir owned by 1001. A new named volume mounted here
# will inherit this ownership on first init, so spark can mkdir subpaths.
RUN mkdir -p /checkpoints && chown -R 1001:1001 /checkpoints

WORKDIR /app
COPY streaming_etl.py .
COPY backfill.py .
RUN chown -R 1001:1001 /app

USER 1001

# Belt-and-braces: bypass the OS lookup entirely if /etc/passwd is still missing the entry.
ENV HADOOP_USER_NAME=spark
ENV PYSPARK_PYTHON=python3

ENTRYPOINT ["/opt/bitnami/spark/bin/spark-submit", \
    "--master", "local[*]", \
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.mysql:mysql-connector-j:8.3.0", \
    "--conf", "spark.jars.ivy=/tmp/.ivy2", \
    "--conf", "spark.sql.shuffle.partitions=4", \
    "/app/streaming_etl.py"]
