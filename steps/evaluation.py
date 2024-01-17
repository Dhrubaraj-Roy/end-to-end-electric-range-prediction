from datetime import timedelta
import logging
import pandas as pd
from typing import Annotated, Tuple
from prefect import task, Flow
from comet_ml import Experiment
from sklearn.base import RegressorMixin
from model.evaluation import MSE, RMSE, R2Score
from prefect.tasks import task_input_hash

# Create a CometML experiment
experiment = Experiment()
@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def evaluate_model(
    model: RegressorMixin, X_test: pd.DataFrame, y_test: pd.Series
) -> Tuple[Annotated[float, "r2"], 
           Annotated[float, "rmse"],
]:
    """
    Args:
        model: RegressorMixin
        x_test: pd.DataFrame
        y_test: pd.Series
    Returns:
        r2_score: float
        rmse: float
    """
    try:
        prediction = model.predict(X_test)

        # Using the MSE class for mean squared error calculation
        mse_class = MSE()
        mse = mse_class.calculate_score(y_test, prediction)
        experiment.log_metric("MSE", mse)
        # Using the R2Score class for R2 score calculation
        r2_class = R2Score()
        r2 = r2_class.calculate_score(y_test, prediction)
        experiment.log_metric("R2Score", r2)
        # Using the RMSE class for root mean squared error calculation
        rmse_class = RMSE()
        rmse = rmse_class.calculate_score(y_test, prediction)
        experiment.log_metric("RMSE", rmse)
       # Log metrics to CometML
       
        
        
        
        experiment.log_metric("model_evaluation_status", 1)
        print("Evaluate model finished")

        return r2, rmse
    except Exception as e:
        logging.error(f"Error in evaluation: {e}")
        raise e
    finally:
        # Ensure that the experiment is ended to log all data
        experiment.end()
