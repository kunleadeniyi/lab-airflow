import logging

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    style="%",
    datefmt="%Y-%m-%d %H:%M"
)
# logger = logging.getLogger(__name__)
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.DEBUG)