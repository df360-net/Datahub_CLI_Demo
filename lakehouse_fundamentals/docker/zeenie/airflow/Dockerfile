# Airflow image for the Sales platform = official base + Docker CLI (to docker-exec the Spark jobs)
# + the Python deps the Sales tools need (reusing Airflow's own SQLAlchemy 1.4; the tools use
# future=True so 2.0-style code runs on 1.4). See docs/Sales_Orchestration_Design.md.
FROM apache/airflow:2.10.5
USER root
# docker.io = the Docker CLI (talks to the host daemon via the mounted socket); sudo so the airflow
# user can reach the root-owned socket.
RUN apt-get update \
 && apt-get install -y --no-install-recommends docker.io sudo \
 && echo "airflow ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/airflow \
 && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir pymysql cryptography boto3 faker python-dotenv
