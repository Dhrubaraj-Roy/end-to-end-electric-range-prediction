import streamlit as st
import pickle
from pydantic import BaseModel

# Define our input data class
class InputData(BaseModel):
    Postal_Code: int
    Model_Year: int
    Base_MSRP: int
    Legislative_District: int

# Load the trained model from the Pickle file
model_filename = "trained_model.pkl"
with open(model_filename, 'rb') as model_file:
    trained_model = pickle.load(model_file)

# Streamlit App
def main():
    st.title("Electric Range Prediction App")

    # User input for prediction
    st.header("Enter Data for Prediction")
    postal_code = st.number_input("Postal Code", min_value=0, step=1)
    model_year = st.number_input("Model Year", min_value=0, step=1)
    base_msrp = st.number_input("Base MSRP", min_value=0, step=1)
    legislative_district = st.number_input("Legislative District", min_value=0, step=1)

    input_data = InputData(
        Postal_Code=postal_code,
        Model_Year=model_year,
        Base_MSRP=base_msrp,
        Legislative_District=legislative_district
    )

    if st.button("Predict"):
        # Use the loaded model to make predictions
        prediction = trained_model.predict([[input_data.Postal_Code, input_data.Model_Year, input_data.Base_MSRP, input_data.Legislative_District]])
        
        # Display the prediction
        st.header("Prediction Result")
        st.write(prediction)

if __name__ == "__main__":
    main()
