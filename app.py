# """
# Created on Tue Nov 17 21:40:41 2020

# @author: win10
# """

# # 1. Library imports
# import uvicorn
# from fastapi import FastAPI, Request
# from model import mymodel
# import numpy as np
# import pickle
# import pandas as pd
# # 2. Create the app object
# app = FastAPI()
# pickle_in = open("/home/dhruba/gigs_project/project_b/FacilityFinder/src/regression.pkl","rb")
# regression=pickle.load(pickle_in)

# # 3. Index route, opens automatically on http://127.0.0.1:8000
# @app.get('/')
# def index():
#     return {'message': 'Hello, World'}

# # 4. Route with a single parameter, returns the parameter within a message
# #    Located at: http://127.0.0.1:8000/AnyNameHere
# @app.get('/{name}')
# def get_name(name: str):
#     return {'Welcome To Krish Youtube Channel': f'{name}'}

# # 3. Expose the prediction functionality, make a prediction from the passed
# #    JSON data and return the predicted Bank Note with the confidence
# @app.post('/predict')
# def predict_banknote(data:mymodel):
#     data = data.dict()
#     PostalCode=data['PostalCode']
#     ModelYea=data['ModelYea']
#     LegislativeDistrict=data['LegislativeDistrict']
#     CensusTract=data['CensusTract']
   

#     # Perform the regression prediction using the loaded model
#     prediction =  regression.predict([[PostalCode,ModelYea,LegislativeDistrict,CensusTract]])

#     return {"prediction": prediction[0]}

# # 5. Run the API with uvicorn
# #    Will run on http://127.0.0.1:8000
# if __name__ == '__main__':
#     uvicorn.run(app, host='127.0.0.1', port=8000)
#     #uvicorn app:app --reload

# 1. Library imports
import uvicorn
from fastapi import FastAPI, Request
from pydantic import BaseModel
import numpy as np
import joblib

# 2. Create the app object
app = FastAPI()

# Load the trained regression model using joblib
regression = joblib.load("/home/dhruba/gigs_project/project_b/FacilityFinder/src/regression.pkl")

# 3. Index route, opens automatically on http://127.0.0.1:8000
@app.get('/')
def index():
    return {'message': 'Hello, World'}

# 4. Route with a single parameter, returns the parameter within a message
@app.get('/{name}')
def get_name(name: str):
    return {'Welcome To Krish Youtube Channel': f'{name}'}

# 5. Expose the prediction functionality
class InputData(BaseModel):
    Postal_Code: int
    Model_Year: int
    Legislative_District: int
   

@app.post('/predict')
def predict_regression(data: InputData):
    Postal_Code = data.Postal_Code
    Model_Year = data.Model_Year
    Legislative_District = data.Legislative_District
 

    # Perform the regression prediction using the loaded model
    prediction = regression.predict([[Postal_Code, Model_Year, Legislative_District]])

    return {"prediction": prediction[0]}

# 6. Run the API with uvicorn
if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8000)
 #uvicorn app:app --reload