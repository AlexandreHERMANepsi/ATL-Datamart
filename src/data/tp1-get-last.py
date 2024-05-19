import requests
from minio import Minio

# Replace with your Minio server endpoint and access key
minio_endpoint = "your-minio-server-endpoint"
minio_access_key = "your-minio-access-key"
minio_secret_key = "your-minio-secret-key"

# Create a Minio client
minio_client = Minio(minio_endpoint, access_key=minio_access_key, secret_key=minio_secret_key, secure=False)

# Define the data URL
data_url = "https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

# Get the current date
import datetime
today = datetime.date.today()

# Get the latest month and year
latest_month = today.month
latest_year = today.year

# Download the data for the latest month
filename = f"tlc_trip_data_{latest_year}_{latest_month}.csv"

response = requests.get(f"{data_url}?year={latest_year}&month={latest_month}")
data = response.content

# Upload the data to Minio
minio_client.put_object(bucket_name="nyc-taxi-data", object_name=filename, data=data, length=len(data))

print(f"Uploaded {filename} to Minio")
