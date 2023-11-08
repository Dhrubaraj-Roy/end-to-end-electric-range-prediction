from fastapi import FastAPI
import joblib

app = FastAPI()

# Load the trained regression model
model = joblib.load('../model/trained_regression_model.pkl')

@app.get("/")
def read_root():
    return {"message": "Regression Model API"}

@app.post("/predict/")
def predict(data: dict):
    try:
        # Extract features from the 'data' dictionary
        # Ensure that the feature names match the ones used during model training
        features = [data['feature1'], data['feature2'], ...]  # Replace with your feature names

        # Perform prediction using the loaded model
        prediction = model.predict([features])

        return {"prediction": prediction[0]}
    except Exception as e:
        return {"error": str(e)}
