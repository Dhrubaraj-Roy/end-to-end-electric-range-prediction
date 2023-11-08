import pandas as pd
from sklearn.metrics import mean_squared_error, r2_score
import joblib
from sklearn.model_selection import train_test_split

def evaluate_model(model_file_path, X_test, y_test):
    model = joblib.load(model_file_path)
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    return mse, r2

def main():
    csv_file_path = '/home/dhruba/gigs_project/project_b/FacilityFinder/data/processed/processed.csv'
    model_file_path = 'regression.pkl'

    df = pd.read_csv(csv_file_path)
    features = ['Postal_Code', 'Model_Year', 'Legislative_District']
    target_column = 'Electric Range'
    X = df[features]
    y = df[target_column]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    mse, r2 = evaluate_model(model_file_path, X_test, y_test)

    print(f"Mean Squared Error: {mse}")
    print(f"R-squared: {r2}")

if __name__ == "__main__":
    main()
