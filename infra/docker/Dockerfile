FROM apache/airflow:2.8.0-python3.10

# 1. Install python dependencies as airflow
USER airflow
COPY infra/docker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# 2. Switch to root to install OS dependencies using playwright's own script
USER root
RUN apt-get update \
    && PYTHONPATH=/home/airflow/.local/lib/python3.10/site-packages /home/airflow/.local/bin/playwright install-deps chromium \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 3. Switch back to airflow to install the browser binaries
USER airflow
RUN /home/airflow/.local/bin/playwright install chromium
