import logging
import pickle
import pandas as pd
from sklearn.base import RegressorMixin
from prefect import task, Flow
from comet_ml import Experiment
from model.model_dev import LinearRegressionModel
from .config import ModelNameConfig
from prefect.tasks import task_input_hash
from datetime import timedelta
import joblib

# Create a CometML experiment
experiment = Experiment()
@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def train_model(
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    y_train: pd.Series,
    y_test: pd.Series,
    config: ModelNameConfig = ModelNameConfig(),
) -> RegressorMixin:
    """
    Train a regression model based on the specified configuration.

    Args:
        X_train (pd.DataFrame): Training data features.
        X_test (pd.DataFrame): Testing data features.
        y_train (pd.Series): Training data target.
        y_test (pd.Series): Testing data target.
        config (ModelNameConfig): Model configuration.

    Returns:
        RegressorMixin: Trained regression model.
    """
    try:
        model = None
        if config.model_name == "linear_regression":
            model = LinearRegressionModel()
            trained_model = model.train(X_train, y_train)
             # Save the trained model to a file
            model_filename = "trained_model.pkl"
            with open(model_filename, 'wb') as model_file:
                pickle.dump(trained_model, model_file)
            print("train model finished")
            experiment.log_metric("model_training_status", 1)
            return trained_model
        else:
            raise ValueError("Model name not supported")
    except Exception as e:
        logging.error(f"Error in train model: {e}")
        raise e
    finally:
    # Ensure that the experiment is ended to log all data
        experiment.end()

