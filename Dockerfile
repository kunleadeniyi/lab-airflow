# use astro image when using astro cli
# FROM astrocrpublic.azurecr.io/runtime:3.0-1

# use extended image when using raw docker-compose
FROM apache/airflow:3.0.1
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt