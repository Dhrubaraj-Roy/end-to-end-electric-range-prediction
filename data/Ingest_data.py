
from azure.storage.blob import BlobServiceClient
import pandas as pd

class AzureBlobFacade:
    def __init__(self, connection_string, container_name, blob_name):
        self.connection_string = connection_string
        self.container_name = container_name
        self.blob_name = blob_name

    def download_blob(self, local_file):
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        blob_client = blob_service_client.get_blob_client(self.container_name, self.blob_name)

        try:
            with open(local_file, "wb") as my_blob:
                my_blob.write(blob_client.download_blob().readall())
            return True
        except Exception as e:
            print(f"Error downloading the blob: {str(e)}")
            return False

class DataProcessor:
    def __init__(self, file_path):
        self.file_path = file_path

    def read_csv(self):
        try:
            df = pd.read_csv(self.file_path)
            return df
        except Exception as e:
            print(f"Error reading the CSV file: {str(e)}")
            return None

def main():
    connection_string = "DefaultEndpointsProtocol=https;AccountName=flightdelay;AccountKey=6Pa5pPXTVPOnmUr0V3jdw+Y2U469ZKvKz/llU2w1BOoBFDG45f0+px5OFOVYO1bH6nGFmS1PQIVC+AStfSq/Lg==;EndpointSuffix=core.windows.net"
    container_name = "electricrange"
    blob_name = "Electric_Vehicle_Population_Data.csv"
    file_path = "Electric_Vehicle_Population_Data.csv"

    azure_blob_facade = AzureBlobFacade(connection_string, container_name, blob_name)
    data_processor = DataProcessor(file_path)

    if azure_blob_facade.download_blob(file_path):
        df = data_processor.read_csv()
        if df is not None:
            print("CSV file connected successfully. Here are the first few rows:")
            print(df.head())

if __name__ == "__main__":
    main()
