FROM apache/airflow:2.8.0-python3.10

# ============================================================
# 1. Install Python dependencies
# ============================================================

USER airflow

COPY infra/docker/requirements.txt /tmp/requirements.txt

RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
        --retries 10 \
        --default-timeout=100 \
        -r /tmp/requirements.txt \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.0/constraints-3.10.txt"

# Heavy ML/NLP dependencies
COPY infra/docker/requirements-nlp.txt /tmp/requirements-nlp.txt

RUN pip install --no-cache-dir \
    --retries 10 \
    --default-timeout=100 \
    -r /tmp/requirements-nlp.txt


# ============================================================
# 2. Install system dependencies
# ============================================================

USER root

# ------------------------------------------------------------
# 2a. Playwright / Chromium dependencies
# ------------------------------------------------------------

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       libnss3 \
       libnspr4 \
       libatk1.0-0 \
       libatk-bridge2.0-0 \
       libcups2 \
       libdrm2 \
       libdbus-1-3 \
       libxkbcommon0 \
       libatspi2.0-0 \
       libxcomposite1 \
       libxdamage1 \
       libxfixes3 \
       libxrandr2 \
       libgbm1 \
       libpango-1.0-0 \
       libcairo2 \
       libasound2 \
       libwayland-client0 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# ============================================================
# 2b. Java 17
# ============================================================

# Java is required by Spark
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       openjdk-17-jre-headless \
       wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Use Debian's stable Java symlink.
# On ARM64:
# /usr/lib/jvm/java-17-current
#     -> /usr/lib/jvm/java-17-openjdk-arm64
#
# On AMD64, Debian resolves the correct architecture automatically.

ENV JAVA_HOME=/usr/lib/jvm/java-17-current

ENV PATH="${JAVA_HOME}/bin:${PATH}"


# ============================================================
# 2c. Install Apache Spark
# ============================================================

ENV SPARK_VERSION=3.5.1
ENV SPARK_HOME=/opt/spark

ENV PATH="${SPARK_HOME}/bin:${JAVA_HOME}/bin:${PATH}"

RUN wget -qO /tmp/spark.tgz \
      "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    && mkdir -p ${SPARK_HOME} \
    && tar -xzf /tmp/spark.tgz \
        -C ${SPARK_HOME} \
        --strip-components=1 \
    && rm -f /tmp/spark.tgz


# ============================================================
# 2d. Spark JAR dependencies
# ============================================================

RUN wget -q \
      https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_2.12/0.15.0/hudi-spark3.5-bundle_2.12-0.15.0.jar \
      -P ${SPARK_HOME}/jars/ \
    && wget -q \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      -P ${SPARK_HOME}/jars/ \
    && wget -q \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      -P ${SPARK_HOME}/jars/ \
    && wget -q \
      https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.11.0/elasticsearch-spark-30_2.12-8.11.0.jar \
      -P ${SPARK_HOME}/jars/ \
    && wget -q \
      https://repo1.maven.org/maven2/io/qdrant/spark/2.5.0/spark-2.5.0.jar \
      -P ${SPARK_HOME}/jars/


# ============================================================
# 2e. Apache Tika
# ============================================================

RUN mkdir -p /opt/tika \
    && wget -q \
       "https://search.maven.org/remotecontent?filepath=org/apache/tika/tika-server-standard/3.1.0/tika-server-standard-3.1.0.jar" \
       -O /opt/tika/tika-server.jar \
    && wget -q \
       "https://search.maven.org/remotecontent?filepath=org/apache/tika/tika-server-standard/3.1.0/tika-server-standard-3.1.0.jar.md5" \
       -O /opt/tika/tika-server.jar.md5

ENV TIKA_SERVER_JAR="file:///opt/tika/tika-server.jar"


# ============================================================
# 3. Return to Airflow user
# ============================================================

USER airflow

# Install Chromium for Playwright
RUN /home/airflow/.local/bin/playwright install chromium


# ============================================================
# 4. HuggingFace model
# ============================================================

ENV HF_HOME=/opt/airflow/.cache/huggingface

RUN python -c \
    "from sentence_transformers import SentenceTransformer; \
     SentenceTransformer('intfloat/multilingual-e5-small')"
