FROM apache/airflow:2.8.0-python3.10

# 1. Install python dependencies as airflow
USER airflow
COPY infra/docker/requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --retries 10 --default-timeout=100 -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.0/constraints-3.10.txt"

# 1b. Install heavy ML/NLP dependencies WITHOUT Airflow constraints to prevent backtracking loops
COPY infra/docker/requirements-nlp.txt /tmp/requirements-nlp.txt
RUN pip install --no-cache-dir --retries 10 --default-timeout=100 -r /tmp/requirements-nlp.txt

# 2. Switch to root to install OS dependencies (Playwright Chromium deps + Java + Spark)
USER root

# 2a. Playwright browser dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 \
       libcups2 libdrm2 libdbus-1-3 libxkbcommon0 \
       libatspi2.0-0 libxcomposite1 libxdamage1 libxfixes3 \
       libxrandr2 libgbm1 libpango-1.0-0 libcairo2 libasound2 \
       libwayland-client0 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2b. Install Java 17 JRE (required for spark-submit client mode)
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# 2c. Install Spark 3.5.1 binaries (client mode — no cluster daemons needed)
ENV SPARK_VERSION=3.5.1
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN wget -qO /tmp/spark.tgz \
      "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    && mkdir -p ${SPARK_HOME} \
    && tar -xzf /tmp/spark.tgz -C ${SPARK_HOME} --strip-components=1 \
    && rm /tmp/spark.tgz

# 2d. Download required JARs into Spark jars directory
RUN wget -q https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_2.12/0.15.0/hudi-spark3.5-bundle_2.12-0.15.0.jar \
         -P ${SPARK_HOME}/jars/ \
    && wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
         -P ${SPARK_HOME}/jars/ \
    && wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
         -P ${SPARK_HOME}/jars/ \
    && wget -q https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.11.0/elasticsearch-spark-30_2.12-8.11.0.jar \
         -P ${SPARK_HOME}/jars/ \
    && wget -q https://repo1.maven.org/maven2/io/qdrant/spark/2.5.0/spark-2.5.0.jar \
         -P ${SPARK_HOME}/jars/

# 2e. Pre-download Tika Server JAR to prevent concurrent downloads by PySpark tasks
RUN mkdir -p /opt/tika \
    && wget -q https://search.maven.org/remotecontent?filepath=org/apache/tika/tika-server-standard/3.1.0/tika-server-standard-3.1.0.jar \
         -O /opt/tika/tika-server.jar \
    && wget -q https://search.maven.org/remotecontent?filepath=org/apache/tika/tika-server-standard/3.1.0/tika-server-standard-3.1.0.jar.md5 \
         -O /opt/tika/tika-server.jar.md5

ENV TIKA_SERVER_JAR="file:///opt/tika/tika-server.jar"

# 3. Switch back to airflow to install the browser binaries
USER airflow
RUN /home/airflow/.local/bin/playwright install chromium
