from datetime import timedelta
from prefect.tasks import task_input_hash
import logging
import comet_ml
import pandas as pd
from prefect import task, Flow
from comet_ml import Experiment
from comet_ml import config
experiment = Experiment()
class IngestData:
    """Ingests data from a CSV file."""

    def __init__(self, data_path: str):
        self.data_path = data_path

    def get_data(self):
        logging.info(f"Ingest data from {self.data_path}")
        return pd.read_csv(self.data_path)

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def ingest_df(data_path: str) -> pd.DataFrame:
    """
    Ingest data from the specified path and return a DataFrame.

    Args:
        data_path (str): The path to the data file.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the ingested data.
    """
    try:
        ingest_obj = IngestData(data_path)
        df = ingest_obj.get_data()
        print(f"Ingesting data from {data_path}")
        experiment.log_metric("data_ingestion_status", 1)
        return df
    except Exception as e:
        logging.error(f"Error while ingesting data: {e}")
        raise e
    finally:
        # Ensure that the experiment is ended to log all data
        experiment.end()