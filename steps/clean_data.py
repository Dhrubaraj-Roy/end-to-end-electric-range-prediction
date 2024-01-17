import logging
import pandas as pd
from typing import Tuple
from typing_extensions import Annotated
from prefect import task, Flow
from comet_ml import Experiment
from model.data_cleaning import (
    DataCleaning,
    DataDivideStrategy,
    DataPreprocessStrategy,
)
from prefect.tasks import task_input_hash
from datetime import timedelta
# Create a CometML experiment
experiment = Experiment()
@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def clean_df(data: pd.DataFrame) -> Tuple[
    Annotated[pd.DataFrame, 'X_train'],
    Annotated[pd.DataFrame, 'X_test'],
    Annotated[pd.Series, 'y_train'],
    Annotated[pd.Series, 'y_test'],
]:
    """
    Data cleaning class which preprocesses the data and divides it into train and test data.

    Args:
        data: pd.DataFrame
    """
    try:
        preprocess_strategy = DataPreprocessStrategy()
        data_cleaning = DataCleaning(data, preprocess_strategy)
        preprocessed_data = data_cleaning.handle_data()

        divide_strategy = DataDivideStrategy()
        data_cleaning = DataCleaning(preprocessed_data, divide_strategy)
        X_train, X_test, y_train, y_test = data_cleaning.handle_data()
        logging.info(f"Data Cleaning Complete")
        experiment.log_metric("data_cleaning_status", 1)
        return X_train, X_test, y_train, y_test 
    except Exception as e: 
        logging.error(e)
        raise e
    finally:
        # Ensure that the experiment is ended to log all data
        experiment.end()

