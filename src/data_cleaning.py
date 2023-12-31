import pandas as pd
import numpy as np
from scipy import stats
# Read the dataset from a CSV file
df = pd.read_csv('/home/dhruba/gigs_project/project_b/FacilityFinder/data/raw/Electric_Vehicle_Population_Data.csv')

# Handling missing values
# Identify missing values in the dataset
missing_data = df.isnull().sum()

# Define a threshold for missing values (e.g., 5%)
threshold = 0.05  # we can adjust this threshold as needed

# Create a list of columns with missing values exceeding the threshold
columns_to_drop = missing_data[missing_data / len(df) > threshold].index.tolist()

# Drop columns with too many missing values
df = df.drop(columns=columns_to_drop)

# Handle missing values in specific columns
# For columns with numeric data, we can fill missing values with the mean
numeric_cols = df.select_dtypes(include=[np.number]).columns
df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].mean())

# For categorical columns, we can fill missing values with the most frequent category
categorical_cols = df.select_dtypes(exclude=[np.number]).columns
df[categorical_cols] = df[categorical_cols].fillna(df[categorical_cols].mode().iloc[0])

# Handling missing values in the target variable (Electric Range)
# It's important to check if the target variable has any missing values
target_variable = 'Electric Range'

if df[target_variable].isnull().any():
    # we can choose to drop rows with missing target values or impute them
    # To drop rows with missing target values:
    df = df.dropna(subset=[target_variable])

    # To impute missing target values with the mean:
    # target_mean = df[target_variable].mean()
    # df[target_variable].fillna(target_mean, inplace=True)

# Now, wer 'df' dataset should have missing values handled
# we can proceed with further data preprocessing and modeling

# Save the cleaned dataset to a new file

cleaned_file_path = '/home/dhruba/gigs_project/project_b/FacilityFinder/data/processed/cleaned_data.csv'  # Update with the desired file path
df.to_csv(cleaned_file_path, index=False)




# Dealing with Outliers
# Define the columns in your dataset that you want to check for outliers
# In this case, we'll focus on numeric columns
numeric_cols = df.select_dtypes(include=[np.number]).columns

# Function to detect and handle outliers using the Z-score method
def handle_outliers_zscore(data, column, threshold=3):
    z_scores = np.abs(stats.zscore(data[column]))
    outliers = data[z_scores > threshold]
    data[column] = data[column].clip(lower=outliers[column].min(), upper=outliers[column].max())
    return data

# Loop through each numeric column and handle outliers
for column in numeric_cols:
    df = handle_outliers_zscore(df, column)

# You can also handle outliers using other methods such as IQR (Interquartile Range)

# Function to detect and handle outliers using IQR method
def handle_outliers_iqr(data, column, multiplier=1.5):
    Q1 = data[column].quantile(0.25)
    Q3 = data[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - multiplier * IQR
    upper_bound = Q3 + multiplier * IQR
    data[column] = data[column].clip(lower=lower_bound, upper=upper_bound)
    return data

# Loop through each numeric column and handle outliers using IQR
for column in numeric_cols:
    df = handle_outliers_iqr(df, column)

# Now, your 'df' dataset should have outliers handled
# You can proceed with further data preprocessing and modeling


# Data Type Conversion
# Convert data types for columns in your DataFrame
df['Postal Code'] = df['Postal Code'].astype(int)
df['Model Year'] = df['Model Year'].astype(int)
df['Electric Range'] = df['Electric Range'].astype(int)
df['Base MSRP'] = df['Base MSRP'].astype(float)  # Assuming this is a floating-point value

# If 'Legislative District' should be treated as a category, you can convert it to a string
df['Legislative District'] = df['Legislative District'].astype(str)

# You can handle other columns similarly, specifying their correct data types

# Now, your DataFrame has the appropriate data types

# Save the DataFrame to a new file with the converted data types
# Replace 'converted_dataset.csv' with your desired file name

cleaned_file_path = '/home/dhruba/gigs_project/project_b/FacilityFinder/data/processed/cleaned_data.csv'  # Update with the desired file path
df.to_csv(cleaned_file_path, index=False)




# Handling Duplicates
# Identify and remove duplicate rows
df_duplicates = df[df.duplicated(keep='first')]

# Print the duplicate rows
print("Duplicate Rows:")
print(df_duplicates)

# Remove duplicate rows from the dataset
df = df.drop_duplicates(keep='first')

# Save the dataset without duplicates to a new file

cleaned_file_path = '/home/dhruba/gigs_project/project_b/FacilityFinder/data/processed/cleaned_data.csv'  # Update with the desired file path
df.to_csv(cleaned_file_path, index=False)

print(df.head())
print(df.shape)



#Addressing Inconsistencies
# Addressing inconsistencies in the 'County' column
# Convert all county names to title case (e.g., "KING" to "King")
df['County'] = df['County'].str.title()

# Addressing inconsistencies in the 'City' column
# Convert all city names to title case
df['City'] = df['City'].str.title()

# Addressing inconsistencies in the 'State' column
# Convert all state abbreviations to uppercase (e.g., "wa" to "WA")
df['State'] = df['State'].str.upper()

# Addressing inconsistencies in the 'Make' and 'Model' columns
# Convert make and model names to title case
df['Make'] = df['Make'].str.title()
df['Model'] = df['Model'].str.title()

# Addressing inconsistencies in 'Electric Vehicle Type' column
# Remove leading and trailing whitespaces
df['Electric Vehicle Type'] = df['Electric Vehicle Type'].str.strip()

# Addressing inconsistencies in 'Clean Alternative Fuel Vehicle (CAFV) Eligibility' column
# Standardize the values to 'Eligible' and 'Not Eligible'
df['Clean Alternative Fuel Vehicle (CAFV) Eligibility'] = df['Clean Alternative Fuel Vehicle (CAFV) Eligibility'].str.lower()
df['Clean Alternative Fuel Vehicle (CAFV) Eligibility'] = df['Clean Alternative Fuel Vehicle (CAFV) Eligibility'].map({'eligible': 'Eligible', 'not eligible due to low battery range': 'Not Eligible'})

# Addressing inconsistencies in 'Vehicle Location' column
# Remove leading and trailing whitespaces
df['Vehicle Location'] = df['Vehicle Location'].str.strip()

# Addressing inconsistencies in 'Electric Utility' column
# Convert electric utility names to title case
df['Electric Utility'] = df['Electric Utility'].str.title()

# Now, your dataset should have inconsistencies addressed
# You can proceed with further data preprocessing and analysis
cleaned_file_path = '/home/dhruba/gigs_project/project_b/FacilityFinder/data/processed/cleaned_data.csv'  # Update with the desired file path
df.to_csv(cleaned_file_path, index=False)


# Data Scaling and Normalization
from sklearn.preprocessing import StandardScaler, MinMaxScaler
import pandas as pd

# Select the numeric columns you want to scale and normalize
numeric_cols = df.select_dtypes(include=[np.number]).columns

# Create a copy of the dataset to avoid modifying the original data
df_scaled = df.copy()

# Standardization (scaling to mean=0, std=1)
scaler = StandardScaler()
df_scaled[numeric_cols] = scaler.fit_transform(df[numeric_cols])

# Min-Max Scaling (scaling to a specific range, e.g., [0, 1])
min_max_scaler = MinMaxScaler()
df_scaled[numeric_cols] = min_max_scaler.fit_transform(df[numeric_cols])

# Now, your 'df_scaled' dataset contains scaled and normalized numeric features
# You can proceed with further analysis, including modeling

# Save the scaled and normalized dataset to a new file
# Replace 'scaled_dataset.csv' with the desired file name
df_scaled.to_csv('/home/dhruba/gigs_project/project_b/FacilityFinder/data/processed/scaled_dataset.csv', index=False)



 # Feature Selection

from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import f_regression
# Define the target variable
target_variable = 'Electric Range'

# Define the feature columns (exclude non-numeric and non-target columns)
feature_columns = df.select_dtypes(include=['number']).columns
feature_columns = feature_columns[feature_columns != target_variable]

# Separate features (X) and the target variable (y)
X = df[feature_columns]
y = df[target_variable]

# Select the top 'k' features using SelectKBest and f_regression score
# You can adjust 'k' based on your requirements
k = 5  # Replace with the desired number of features

selector = SelectKBest(score_func=f_regression, k=k)
X_new = selector.fit_transform(X, y)

# Get the indices of the selected features
selected_feature_indices = selector.get_support(indices=True)

# Get the names of the selected features
selected_features = X.columns[selected_feature_indices]

# Print or use the selected features
print("Selected Features:")
print(selected_features)

# You can now use 'X_new' as the dataset with only the selected features for modeling.
 # Data Encoding
from sklearn.preprocessing import LabelEncoder, OneHotEncoder

# Load your dataset (if not already loaded)
# df = pd.read_csv('cleaned_dataset.csv')  # Make sure to load your cleaned dataset

# Select categorical columns that need encoding
categorical_columns = ['County', 'City', 'State', 'Make', 'Model', 'Electric Vehicle Type', 'Clean Alternative Fuel Vehicle (CAFV) Eligibility']

# Initialize LabelEncoder and OneHotEncoder
label_encoder = LabelEncoder()
onehot_encoder = OneHotEncoder(sparse=False, drop='first')

# Label Encoding for selected categorical columns
for col in categorical_columns:
    df[col] = label_encoder.fit_transform(df[col])

# One-Hot Encoding for selected categorical columns
onehot_encoded = onehot_encoder.fit_transform(df[categorical_columns].values.reshape(-1, 1))

# Create column names for one-hot encoded features
onehot_columns = onehot_encoder.get_feature_names_out([categorical_columns[0]])

# Create a DataFrame from one-hot encoding results with appropriate column names
onehot_encoded_df = pd.DataFrame(onehot_encoded, columns=onehot_columns)

# Concatenate the one-hot encoded DataFrame with the original DataFrame
df = pd.concat([df, onehot_encoded_df], axis=1)

# Drop the original categorical columns since they have been encoded
df = df.drop(columns=categorical_columns)

# Now, your dataset has categorical variables encoded for machine learning

# You can proceed with further data preprocessing and modeling


print("your shape", df.shape)
df_scaled.to_csv('/home/dhruba/gigs_project/project_b/FacilityFinder/data/processed/scaled_dataset.csv', index=False)