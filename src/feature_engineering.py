# Import necessary libraries
import pandas as pd
import datetime
# Load the preprocessed dataset
df = pd.read_csv('/home/dhruba/gigs_project/project_b/FacilityFinder/data/raw/part2.csv')

# Feature Engineering

# Example 1: Creating a new feature based on existing ones


# Assuming your dataset has a column named 'Model Year' containing the model year of the vehicles.
current_year = datetime.datetime.now().year
df['Age of Electric Vehicle'] = current_year - df['Model Year']

# Assuming your dataset has columns 'Make' and 'Electric Range'.
average_range_by_make = df.groupby('Make')['Electric Range'].mean().reset_index()
average_range_by_make.rename(columns={'Electric Range': 'Average Electric Range by Make'}, inplace=True)
df = df.merge(average_range_by_make, on='Make', how='left')

# Assuming you want to categorize 'Base MSRP' into three price ranges.
bins = [0, 20000, 40000, float('inf')]
labels = ['Affordable', 'Mid-range', 'Luxury']
df['Price Range Categories'] = pd.cut(df['Base MSRP'], bins=bins, labels=labels, include_lowest=True)


# Assuming your dataset has a column named 'Clean Alternative Fuel Vehicle (CAFV) Eligibility'.
df['Count of Clean Alternative Fuel Vehicle Eligibility'] = df['Clean Alternative Fuel Vehicle (CAFV) Eligibility'].apply(lambda x: 1 if x == 'Clean Alternative Fuel Vehicle Eligible' else 0)


# Example 2: Encoding categorical variables
# You can use one-hot encoding or label encoding based on the variable type

# Assuming 'df' is your DataFrame and 'categorical_columns' is a list of the categorical column names.
# Assuming 'df' is your DataFrame, here's how you can select the categorical columns.
categorical_cols = df.select_dtypes(include=['object']).columns.tolist()

# Perform one-hot encoding
df = pd.get_dummies(df, columns=categorical_cols, drop_first=True)


# Example 3: Binning numerical variables
# df['Age_Group'] = pd.cut(df['Age'], bins=[0, 18, 35, 60, 100], labels=['Child', 'Young Adult', 'Adult', 'Elderly'])
# You can customize the bin edges and labels as per your requirements.
bin_edges = [0, 50, 100, 200, 300, float('inf')]
bin_labels = ['0-50', '51-100', '101-200', '201-300', '301+']

df['Electric Range Category'] = pd.cut(df['Electric Range'], bins=bin_edges, labels=bin_labels)

# This will create a new column 'Electric Range Category' with the specified bins.



# Example 4: Extracting information from date columns
# df['Year'] = df['Date_Column'].dt.year
# df['Month'] = df['Date_Column'].dt.month

# Sadly dataset does not contain a specific date column to extract date-related information from.

# This will create new columns 'Year', 'Month', 'Day', 'Weekday', 'Quarter', and 'IsWeekend' in your DataFrame.


# This will create new columns 'Year', 'Month', 'Day', 'Weekday', 'Quarter', and 'IsWeekend' in your DataFrame.


# This will create new columns 'Year', 'Month', 'Day', 'Weekday', 'Quarter', and 'IsWeekend' in your DataFrame.

# Example 5: Aggregating data
# You can group data by a specific column and calculate statistics
# aggregated_data = df.groupby('Category')['Value'].mean().reset_index()

# Example 6: Text data processing
# You can perform text preprocessing like tokenization, stop-word removal, or sentiment analysis
# Assuming 'df' is your DataFrame

# Text cleaning example

# Assuming 'df' is your DataFrame

# Text cleaning example
# Remove leading and trailing white spaces from the 'City' column
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

# Assuming 'df' is your DataFrame
# Columns that contain text data
text_columns = ['County', 'City', 'Make', 'Model', 'Electric Vehicle Type', 'Clean Alternative Fuel Vehicle (CAFV) Eligibility', 'Vehicle Location', 'Electric Utility']

# Initialize a TF-IDF vectorizer
tfidf_vectorizer = TfidfVectorizer()

# Loop through the text columns and process them
for col in text_columns:
    if col in df:
        df[col] = df[col].astype(str)  # Convert to string in case it's not already
        tfidf_matrix = tfidf_vectorizer.fit_transform(df[col])
        df[col + '_tfidf'] = list(tfidf_matrix)

# Now, your DataFrame 'df' contains TF-IDF features for the specified text columns



# Save the dataset with engineered features
path = '/home/dhruba/gigs_project/project_b/FacilityFinder/data/processed/engineered_data.csv'
df.to_csv(path, index=False)


import pandas as pd
from sklearn.preprocessing import OneHotEncoder


def cat_to_col(data):
    # make a new column by splitting the geography column
    # data["Colname"] = [i.split(",")[0] for i in data["Colname"]]
    # data["Colname"] = [i.split(",")[1] for i in data["Colname"]]
    # drop the geography column
    # data.drop("geography", axis=1, inplace=True)
    # return data
    pass


def one_hot_encoding(X):
    # select categorical columns
    categorical_columns = X.select_dtypes(include=["object"]).columns
    # one hot encode categorical columns
    one_hot_encoder = OneHotEncoder(sparse=False, handle_unknown="ignore")
    one_hot_encoded = one_hot_encoder.fit_transform(X[categorical_columns])
    # convert the one hot encoded array to a dataframe
    one_hot_encoded = pd.DataFrame(
        one_hot_encoded, columns=one_hot_encoder.get_feature_names_out(categorical_columns)
    )
    # drop the categorical columns from the original dataframe
    X = X.drop(categorical_columns, axis=1)
    # concatenate the one hot encoded dataframe to the original dataframe
    X = pd.concat([X, one_hot_encoded], axis=1)
    return X