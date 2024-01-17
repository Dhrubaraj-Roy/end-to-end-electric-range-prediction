import logging
import comet_ml
import pandas as pd
from prefect import task, Flow
from comet_ml import Experiment
from comet_ml import config
# import os
# from comet_ml.config import ConfigIniEnv



# experiment = comet_ml.Experiment(
#     api_key=config.get_config()["api_key"],
#     project_name=config.get_config()["project_name"],
#     workspace=config.get_config()["workspace"]  
# )
experiment = Experiment()
class IngestData:
    """Ingests data from a CSV file."""

    def __init__(self, data_path: str):
        self.data_path = data_path

    def get_data(self):
        logging.info(f"Ingest data from {self.data_path}")
        return pd.read_csv(self.data_path)

@task
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