import logging
from abc import ABC, abstractmethod
from typing import Union

import numpy as np
import pandas as pd
from scipy import stats
from sklearn.model_selection import train_test_split


class DataStrategy(ABC):
    """
    Abstract Class defining strategy for handling data
    """

    @abstractmethod
    def handle_data(self, data: pd.DataFrame) -> Union[pd.DataFrame, pd.Series]:
        pass


class DataPreprocessStrategy(DataStrategy):
    """
    Data preprocessing strategy which preprocesses the data.
    """

    def handle_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Removes columns which are not required, fills missing values with median average values, and converts the data type to float.
        """
        try:
            data = data.drop(
                [
                    "VIN (1-10)",
                    "City",
                    "State",
                    "Make",
                    "Model",
                    "Electric Vehicle Type",
                    "Clean Alternative Fuel Vehicle (CAFV) Eligibility",
                    "DOL Vehicle ID",
                    "Vehicle Location",
                    "Electric Utility",
                    "2020 Census Tract",
                ],
                axis=1,
            )
            data["Postal Code"].fillna(data["Postal Code"].median(), inplace=True)
            data["Model Year"].fillna(data["Model Year"].median(), inplace=True)
            data["Electric Range"].fillna(data["Electric Range"].median(), inplace=True)
            data["Base MSRP"].fillna(data["Base MSRP"].median(), inplace=True)
            data["Legislative District"].fillna(data["Legislative District"].median(), inplace=True)
            
            data = data.select_dtypes(include=[np.number])
           
            return data
        except Exception as e:
            logging.error("Error in Data handling: {}".format(e))
            raise e



class DataDivideStrategy(DataStrategy):
    """
    Data dividing strategy which divides the data into train and test data.
    """

    def handle_data(self, data: pd.DataFrame) -> Union[pd.DataFrame, pd.Series]:
        """
        Divides the data into train and test data.
        """
        try:
            X = data.drop("Electric Range", axis=1)
            y = data["Electric Range"]
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42
            )
            return X_train, X_test, y_train, y_test
        except Exception as e:
            logging.error("Error in data divide strategy: {}".format(e))
            raise e


class DataCleaning:
    """
    Data cleaning class which preprocesses the data and divides it into train and test data.
    """

    def __init__(self, data: pd.DataFrame, strategy: DataStrategy) -> None:
        """Initializes the DataCleaning class with a specific strategy."""
        self.df = data
        self.strategy = strategy

    def handle_data(self) -> Union[pd.DataFrame, pd.Series]:
        """Handle data based on the provided strategy"""
        return self.strategy.handle_data(self.df)