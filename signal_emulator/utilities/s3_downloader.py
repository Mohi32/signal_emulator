from dataclasses import dataclass
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
import csv
from io import StringIO
from datetime import datetime

class S3Downloader:
    def __init__(self, bucket_name, aws_credentials=None):
        if aws_credentials:
            self.s3_resource = boto3.resource(
                "s3",
                region_name=aws_credentials.region_name,
                aws_access_key_id=aws_credentials.aws_access_key_id,
                aws_secret_access_key=aws_credentials.aws_secret_access_key,
            )
        else:
            self.s3_resource = boto3.resource("s3")
        self.bucket = self.s3_resource.Bucket(bucket_name)

    def set_bucket(self, bucket_name):
        self.bucket = self.s3_resource.Bucket(bucket_name)

    def download_by_key(self, key, download_path):
        try:
            self.bucket.download_file(key, download_path)
            print(f"Object: {key} successfully downloaded to: {download_path} ")
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                print(f"Object not found exception: {e}")
            else:
                print(f"Other exception: {e}")

    def get_object_list(self, prefix):
        return list(self.bucket.objects.filter(Prefix=prefix).all())

    def download_all_with_prefix(self, prefix, download_directory):
        s3_objects = self.get_object_list(prefix)
        for s3_object in s3_objects:
            output_path = Path(download_directory, s3_object.bucket_name, s3_object.key)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            self.download_by_key(
                key=s3_object.key,
                download_path=output_path,
            )

    def download_csv_to_list(self, key):
        # Download the CSV file from S3
        response = self.bucket.Object(key)
        csv_data = response.get()['Body'].read().decode('utf-8')
        # Create a StringIO object to simulate a file-like object
        csv_file = StringIO(csv_data)
        # Parse the CSV data into a csv.reader object
        csv_reader = csv.reader(csv_file)
        # Now, you can iterate over the csv_reader and work with the CSV data
        output_list = []
        for row in csv_reader:
            output_list.append(row)
        return output_list


@dataclass
class AwsCredentials:
    aws_access_key_id: str
    aws_secret_access_key: str
    region_name: str


if __name__ == "__main__":
    s3_downloader = S3Downloader(
        bucket_name="surface.data.tfl.gov.uk",
        aws_credentials=AwsCredentials(
            aws_access_key_id="***REMOVED***",
            aws_secret_access_key="***REMOVED***",
            region_name="eu-west-1",
        ),
    )
    t = datetime.now()
    csv_data = s3_downloader.download_csv_to_list(key="Control/UTC/SignalEvents/CNTR/2023/10/01/OCT012023_0000_CNTR.csv")
    s3_downloader.download_all_with_prefix(
        prefix="Control/UTC/SignalEvents/CNTR/2023/05/10", download_directory="D:/s3/"
    )
