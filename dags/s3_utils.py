import boto3
import os

class S3DriverExtractor:
    """
    A class to extract the 'chromedriver' file from an S3 bucket.
    """

    def __init__(self, bucket_name: str, drivers_folder: str, chromedriver_filename: str = 'chromedriver'):
        """
        Initializes the S3DriverExtractor with bucket name, drivers folder, and chromedriver filename.

        Args:
            bucket_name (str): The name of the S3 bucket.
            drivers_folder (str): The name of the folder within the bucket containing the drivers.
            chromedriver_filename (str, optional): The name of the chromedriver file. Defaults to 'chromedriver'.
        """
        self.bucket_name = bucket_name
        self.drivers_folder = drivers_folder
        self.chromedriver_filename = chromedriver_filename
        self.s3_client = boto3.client('s3')

    def extract_driver(self, local_path: str) -> None:
        """
        Extracts the chromedriver file from S3 to the specified local path.

        Args:
            local_path (str): The local path where the chromedriver file should be saved.
        """
        try:
            # Construct the S3 key for the chromedriver file
            s3_key = os.path.join(self.drivers_folder, self.chromedriver_filename)

            # Download the file from S3
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)

            print(f"Successfully extracted chromedriver to {local_path}")

        except Exception as e:
            print(f"Error extracting chromedriver: {e}")

# Example usage:
if __name__ == "__main__":
    bucket_name = 'bluexpress'
    drivers_folder = 'drivers'
    local_path = '/path/to/local/chromedriver'  # Replace with your desired local path

    extractor = S3DriverExtractor(bucket_name, drivers_folder)
    extractor.extract_driver(local_path)
