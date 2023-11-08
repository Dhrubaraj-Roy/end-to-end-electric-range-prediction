# import pandas as pd
# from sklearn.model_selection import train_test_split
# from sklearn.linear_model import LinearRegression
# from sklearn.metrics import mean_squared_error, r2_score
# from sklearn.preprocessing import OneHotEncoder
# import joblib

# # Read your CSV file
# df = pd.read_csv('/home/dhruba/gigs_project/project_b/FacilityFinder/data/processed/processed.csv')

# # Define the list of features based on your column names
# features = [
#     'Postal_Code',
#     'Model_Year',
#     'Legislative_District'
    
# ]

# # Split the data into training and testing sets
# X = df[features]
# y = df['Electric Range']

# # Use OneHotEncoder to handle categorical features
# # If there are categorical features in your dataset, add them here and apply one-hot encoding as needed.

# # Split the processed data into training and testing sets
# X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# # Initialize the Linear Regression model
# model = LinearRegression()

# # Train the model on the training data
# model.fit(X_train, y_train)

# # Make predictions on the test data
# y_pred = model.predict(X_test)

# # Evaluate the model
# mse = mean_squared_error(y_test, y_pred)
# r2 = r2_score(y_test, y_pred)
# joblib.dump(model, 'regression.pkl')

# print(f"Mean Squared Error: {mse}")
# print(f"R-squared: {r2}")

# import pandas as pd
# from sklearn.model_selection import train_test_split
# from sklearn.linear_model import LinearRegression
# from sklearn.metrics import mean_squared_error, r2_score
# import joblib

# class MachineLearningFacade:
#     def __init__(self, csv_file_path, target_column, features):
#         self.df = pd.read_csv(csv_file_path)
#         self.target_column = target_column
#         self.features = features

#     def prepare_data(self):
#         X = self.df[self.features]
#         y = self.df[self.target_column]
#         X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
#         return X_train, X_test, y_train, y_test

#     def train_model(self, X_train, y_train):
#         model = LinearRegression()
#         model.fit(X_train, y_train)
#         return model

#     def evaluate_model(self, model, X_test, y_test):
#         y_pred = model.predict(X_test)
#         mse = mean_squared_error(y_test, y_pred)
#         r2 = r2_score(y_test, y_pred)
#         return mse, r2

#     def save_model(self, model, model_file_path):
#         joblib.dump(model, model_file_path)

# def main():
#     csv_file_path = '/home/dhruba/gigs_project/project_b/FacilityFinder/data/processed/processed.csv'
#     target_column = 'Electric Range'
#     features = ['Postal_Code', 'Model_Year', 'Legislative_District']
#     model_file_path = 'regression.pkl'

#     ml_processor = MachineLearningFacade(csv_file_path, target_column, features)
#     X_train, X_test, y_train, y_test = ml_processor.prepare_data()
#     model = ml_processor.train_model(X_train, y_train)
#     mse, r2 = ml_processor.evaluate_model(model, X_test, y_test)
#     ml_processor.save_model(model, model_file_path)

#     print(f"Mean Squared Error: {mse}")
#     print(f"R-squared: {r2}")

# if __name__ == "__main__":
#     main()

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
    csv_file_path = '/home/dhruba/gigs_project/project_b/FacilityFinder/data/processed/processed.csv'
    target_column = 'Electric Range'
    features = ['Postal_Code', 'Model_Year', 'Legislative_District']
    model_file_path = 'regression.pkl'

    ml_processor = MachineLearningFacade(csv_file_path, target_column, features)
    X_train, X_test, y_train, y_test = ml_processor.prepare_data()
    model = ml_processor.train_model(X_train, y_train)
    ml_processor.save_model(model, model_file_path)

if __name__ == "__main__":
    main()
