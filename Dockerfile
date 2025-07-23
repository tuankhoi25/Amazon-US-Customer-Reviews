FROM apache/spark:4.0.0-scala2.13-java17-python3-r-ubuntu

USER root

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

WORKDIR /app
COPY main.py /app/main.py


CMD ["python3", "/app/main.py"]
