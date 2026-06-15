FROM apache/airflow:2.8.0-python3.10

# 1. Install python dependencies as airflow
USER airflow
COPY infra/docker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.0/constraints-3.10.txt"

# 2. Switch to root to install OS dependencies (Playwright Chromium deps)
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 \
       libcups2 libdrm2 libdbus-1-3 libxkbcommon0 \
       libatspi2.0-0 libxcomposite1 libxdamage1 libxfixes3 \
       libxrandr2 libgbm1 libpango-1.0-0 libcairo2 libasound2 \
       libwayland-client0 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 3. Switch back to airflow to install the browser binaries
USER airflow
RUN /home/airflow/.local/bin/playwright install chromium
