import pickle
from src.model_evaluation import model
# Assuming 'model' is your trained model
model = model()

# Save the model to a PKL file
with open("model.pkl", "wb") as file:
    pickle.dump(model, file)
import pickle
from src.model_evaluation import evaluate_model

# Assuming you have evaluated the model in model_evaluation.py and have 'model_file_path', 'X_test', and 'y_test' defined
model_file_path = 'regression.pkl'
X_test = ...  # Your test data
y_test = ...  # Your test labels

# Save the model to a PKL file
with open("model.pkl", "wb") as file:
    pickle.dump(model_file_path, file)
