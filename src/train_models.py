 # Define a threshold for missing values (e.g., 5%)
            threshold = 0.05  # we can adjust this threshold as needed

            # Create a list of columns with missing values exceeding the threshold
            columns_to_drop = missing_data[missing_data / len(data) > threshold].index.tolist()

            # Drop columns with too many missing values
            data = data.drop(columns=columns_to_drop)

            # Handle missing values in specific columns
            # For columns with numeric data, we can fill missing values with the mean
            numeric_cols = data.select_dtypes(include=[np.number]).columns
            data[numeric_cols] = data[numeric_cols].fillna(data[numeric_cols].mean())

            # For categorical columns, we can fill missing values with the most frequent category
            categorical_cols = data.select_dtypes(exclude=[np.number]).columns
            data[categorical_cols] = data[categorical_cols].fillna(data[categorical_cols].mode().iloc[0])
            
            # Dealing with Outliers
            # Define the columns in your dataset that you want to check for outliers
            # In this case, we'll focus on numeric columns
            numeric_cols = data.select_dtypes(include=[np.number]).columns

            # # Function to detect and handle outliers using the Z-score method
            # def handle_outliers_zscore(data, column, threshold=3):
            #     z_scores = np.abs(stats.zscore(data[column]))
            #     outliers = data[z_scores > threshold]
            #     data[column] = data[column].clip(lower=outliers[column].min(), upper=outliers[column].max())
            #     return data

            # Loop through each numeric column and handle outliers
            # for column in numeric_cols:
            #     data = handle_outliers_zscore(data, column)

            # You can also handle outliers using other methods such as IQR (Interquartile Range)

            # Function to detect and handle outliers using IQR method
            # def handle_outliers_iqr(data, column, multiplier=1.5):
            #     Q1 = data[column].quantile(0.25)
            #     Q3 = data[column].quantile(0.75)
            #     IQR = Q3 - Q1
            #     lower_bound = Q1 - multiplier * IQR
            #     upper_bound = Q3 + multiplier * IQR
            #     data[column] = data[column].clip(lower=lower_bound, upper=upper_bound)
            #     return data

            # Loop through each numeric column and handle outliers using IQR
            # for column in numeric_cols:
            #     data = handle_outliers_iqr(data, column)

            # Now, your 'data' dataset should have outliers handled
            # You can proceed with further data preprocessing and modeling


            # Data Type Conversion
            # Convert data types for columns in your DataFrame
            data['Postal Code'] = data['Postal Code'].astype(int)
            data['Model Year'] = data['Model Year'].astype(int)
            data['Electric Range'] = data['Electric Range'].astype(int)
            data['Base MSRP'] = data['Base MSRP'].astype(float)  # Assuming this is a floating-point value

            # If 'Legislative District' should be treated as a category, you can convert it to a string
            data['Legislative District'] = data['Legislative District'].astype(str)
            # Handling Duplicates
            # Identify and remove duplicate rows
            data_duplicates = data[data.duplicated(keep='first')]

            # Print the duplicate rows
            print("Duplicate Rows:")
            print(data_duplicates)

            # Remove duplicate rows from the dataset
            data = data.drop_duplicates(keep='first')
            data = data.drop("VIN (1-10)", axis=1)
            
            # Remove duplicate rows from the dataset
            data = data.drop_duplicates(keep='first')





            #Addressing Inconsistencies
            # Addressing inconsistencies in the 'County' column
            # Convert all county names to title case (e.g., "KING" to "King")
            data['County'] = data['County'].str.title()

            # Addressing inconsistencies in the 'City' column
            # Convert all city names to title case
            data['City'] = data['City'].str.title()

            # Addressing inconsistencies in the 'State' column
            # Convert all state abbreviations to uppercase (e.g., "wa" to "WA")
            data['State'] = data['State'].str.upper()

            # Addressing inconsistencies in the 'Make' and 'Model' columns
            # Convert make and model names to title case
            data['Make'] = data['Make'].str.title()
            data['Model'] = data['Model'].str.title()

            # Addressing inconsistencies in 'Electric Vehicle Type' column
            # Remove leading and trailing whitespaces
            data['Electric Vehicle Type'] = data['Electric Vehicle Type'].str.strip()

            # Addressing inconsistencies in 'Clean Alternative Fuel Vehicle (CAFV) Eligibility' column
            # Standardize the values to 'Eligible' and 'Not Eligible'
            data['Clean Alternative Fuel Vehicle (CAFV) Eligibility'] = data['Clean Alternative Fuel Vehicle (CAFV) Eligibility'].str.lower()
            data['Clean Alternative Fuel Vehicle (CAFV) Eligibility'] = data['Clean Alternative Fuel Vehicle (CAFV) Eligibility'].map({'eligible': 'Eligible', 'not eligible due to low battery range': 'Not Eligible'})

            # Addressing inconsistencies in 'Vehicle Location' column
            # Remove leading and trailing whitespaces
            data['Vehicle Location'] = data['Vehicle Location'].str.strip()

            # Addressing inconsistencies in 'Electric Utility' column
            # Convert electric utility names to title case
            data['Electric Utility'] = data['Electric Utility'].str.title()

            # Data Scaling and Normalization
            from sklearn.preprocessing import StandardScaler, MinMaxScaler
            import pandas as pd

            # Select the numeric columns you want to scale and normalize
            numeric_cols = data.select_dtypes(include=[np.number]).columns

            # Create a copy of the dataset to avoid modifying the original data
            data_scaled = data.copy()

            # Standardization (scaling to mean=0, std=1)
            scaler = StandardScaler()
            data_scaled[numeric_cols] = scaler.fit_transform(data[numeric_cols])

            # Min-Max Scaling (scaling to a specific range, e.g., [0, 1])
            min_max_scaler = MinMaxScaler()
            data_scaled[numeric_cols] = min_max_scaler.fit_transform(data[numeric_cols])

            # Now, your 'data_scaled' dataset contains scaled and normalized numeric features
            # You can proceed with further analysis, including modeling