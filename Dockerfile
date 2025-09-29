FROM apache/airflow:2.9.3-python3.8

USER root

# Cài Java + build tools
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk-headless \
    procps \
    curl \
    gcc \
    g++ \
    make \
    libc-dev \
    python3-dev \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Copy toàn bộ project root vào /opt/airflow/project
WORKDIR /opt/airflow
COPY init/ ./init/
COPY utils/ ./utils/
COPY spark/ ./spark/
COPY requirements.txt .
COPY .env .

# Chuyển sang user airflow để pip install
USER airflow
RUN pip install --no-cache-dir -r requirements.txt

# Thiết lập JAVA_HOME
ENV AWS_REGION=us-east-1
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV PYTHONPATH="/opt/airflow:/opt/airflow/dags:/opt/airflow/plugins"