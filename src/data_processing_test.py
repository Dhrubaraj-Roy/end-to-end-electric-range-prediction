
import pandas as pd
from data_processing import (
    drop_and_fill,
    find_columns_with_few_values,
    find_constant_columns,
)
from feature_engineering import one_hot_encoding

class DataProcessingFacade:
    def __init__(self, csv_file_path):
        self.df = pd.read_csv(csv_file_path)

    def process_data(self):
        print('Original shape:', self.df.shape)
        constant_columns = find_constant_columns(self.df)
        print("Columns that contain a single value: ", constant_columns)
        columns_with_few_values = find_columns_with_few_values(self.df, 10)
        self.df = one_hot_encoding(self.df)
        self.df = drop_and_fill(self.df)
        print('Final shape:', self.df.shape)

    def save_processed_data(self, output_file_path):
        self.df.to_csv(output_file_path, index=False)

def main():
    csv_file_path = '/home/dhruba/gigs_project/project_b/FacilityFinder/data/raw/part2.csv'
    output_file_path = '/home/dhruba/gigs_project/project_b/FacilityFinder/data/processed/processed.csv'

    data_processor = DataProcessingFacade(csv_file_path)
    data_processor.process_data()
    data_processor.save_processed_data(output_file_path)

if __name__ == "__main__":
    main()
