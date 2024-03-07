FROM apache/airflow:2.8.1

USER root

# Copy requirements.txt and install dependencies
COPY requirements.txt /requirements.txt
RUN chown airflow:root /requirements.txt && \
    chmod 777 /requirements.txt

USER airflow

# Install dependencies as airflow user
RUN pip install --no-cache-dir "apache-airflow==2.8.1" -r /requirements.txt

# Switch back to root user for other commands if necessary
USER root
