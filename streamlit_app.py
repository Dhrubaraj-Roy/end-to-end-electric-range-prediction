import streamlit as st
from prefect import Flow, Parameter
from prefect.storage import Local
from prefect.run_configs import LocalRun

# Import your Prefect flow
from flow import my_flow

# Define a Streamlit App
def main():
    st.title("ML Model Prediction App")

    # Add Streamlit components for user input
    user_input = st.text_input("Enter data for prediction:")

    # Use a button to trigger the Prefect flow
    if st.button("Predict"):
        # Define Prefect flow run configuration
        run_config = LocalRun(env={"input_data": user_input})

        # Create a Prefect flow using Local storage and run it
        flow = my_flow()
        flow.storage = Local()
        flow.run_config = run_config

        # Run the Prefect flow with the provided input data
        state = flow.run(parameters={"input_data": user_input})

        # Get the result from the Prefect flow state
        result = state.result[]  # Replace with the actual task name that produces the result

        st.write("Prediction:", result)

if __name__ == "__main__":
    main()
