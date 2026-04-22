FROM apache/airflow:2.8.0
 
USER root
 
# Dépendances système (si besoin pour psycopg2)
RUN apt-get update && apt-get install -y \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*
 
USER airflow
 
# Copier et installer les dépendances Python
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt