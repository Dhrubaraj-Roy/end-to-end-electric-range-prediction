import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import joblib

class MachineLearningFacade:
    def __init__(self, csv_file_path, target_column, features):
        self.df = pd.read_csv(csv_file_path)
        self.target_column = target_column
        self.features = features

    def prepare_data(self):
        X = self.df[self.features]
        y = self.df[self.target_column]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        return X_train, X_test, y_train, y_test

    def train_model(self, X_train, y_train):
        model = LinearRegression()
        model.fit(X_train, y_train)
        return model

    def save_model(self, model, model_file_path):
        joblib.dump(model, model_file_path)

def main():
    csv_file_path = '/home/dhruba/gigs_project/end-to-end-electric-range-prediction/data/processed/processed.csv'
    target_column = 'Electric Range'
    features = ['Postal_Code', 'Model_Year', 'Legislative_District']
    model_file_path = 'regression.pkl'

    ml_processor = MachineLearningFacade(csv_file_path, target_column, features)
    X_train, X_test, y_train, y_test = ml_processor.prepare_data()
    model = ml_processor.train_model(X_train, y_train)
    ml_processor.save_model(model, model_file_path)

if __name__ == "__main__":
    main()
