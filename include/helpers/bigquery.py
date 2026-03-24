import os

from google.cloud import bigquery

from helpers.logger import logger

GCP_PROJECT = os.environ.get('GCP_PROJECT', 'dbt-airflow-project-f1')


def get_bigquery_client():
    """
    Return an authenticated BigQuery client for GCP_PROJECT.

    Authentication is resolved by the Google client library in this order:
      1. GOOGLE_APPLICATION_CREDENTIALS env var (service account JSON key path)
      2. Application Default Credentials (gcloud auth / workload identity)
    """
    logger.info("Connecting to BigQuery project %s", GCP_PROJECT)
    return bigquery.Client(project=GCP_PROJECT)
