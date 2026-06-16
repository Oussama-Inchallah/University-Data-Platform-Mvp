FROM apache/airflow:2.8.0-python3.10

# 1. Install python dependencies as airflow
USER airflow
COPY infra/docker/requirements.txt /tmp/requirements.txt
RUN pip install --default-timeout=1000 --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.0/constraints-3.10.txt"

# 2. Switch to root to install OS dependencies (Playwright Chromium deps + Java for Spark)
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       default-jre-headless \
       libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 \
       libcups2 libdrm2 libdbus-1-3 libxkbcommon0 \
       libatspi2.0-0 libxcomposite1 libxdamage1 libxfixes3 \
       libxrandr2 libgbm1 libpango-1.0-0 libcairo2 libasound2 \
       libwayland-client0 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

# 3. Switch back to airflow to install the browser binaries and Spark Jars
USER airflow
RUN /home/airflow/.local/bin/playwright install chromium

# 4. Download Spark/Hadoop/AWS jars for professional (air-gapped) execution
RUN mkdir -p /opt/airflow/jars && \
    curl -o /opt/airflow/jars/hudi-spark3.5-bundle_2.12-0.15.0.jar https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_2.12/0.15.0/hudi-spark3.5-bundle_2.12-0.15.0.jar && \
    curl -o /opt/airflow/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
