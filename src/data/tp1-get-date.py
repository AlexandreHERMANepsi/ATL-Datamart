import os
import requests
from minio import Minio
from minio.error import S3Error
from datetime import datetime

# Fonction pour télécharger un fichier et le stocker dans Minio
def download_and_upload_to_minio(url, minio_client, bucket_name):
    filename = url.split("/")[-1]
    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, 'wb') as f:
            f.write(response.content)
        try:
            minio_client.fput_object(bucket_name, filename, filename)
            os.remove(filename)  # Supprimer le fichier après l'avoir téléchargé
            print(f"Le fichier {filename} a été téléchargé et stocké dans Minio.")
        except S3Error as e:
            print(f"Une erreur s'est produite lors du stockage dans Minio : {e}")

# Fonction pour récupérer les datasets de janvier 2023 à août 2023
def retrieve_datasets_jan_to_aug_2023(minio_client, bucket_name):
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    for month in range(1, 9):  # De janvier à août
        year_month = f"2023-{month:02d}"
        url = f"{base_url}/yellow_tripdata_{year_month}.parquet"
        download_and_upload_to_minio(url, minio_client, bucket_name)

# Fonction pour récupérer le dernier mois disponible
def retrieve_latest_dataset(minio_client, bucket_name):
    current_date = datetime.now()
    year_month = current_date.strftime("%Y-%m")
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year_month}.parquet"
    download_and_upload_to_minio(url, minio_client, bucket_name)

# Paramètres Minio
minio_endpoint = "127.0.0.1:9000"
minio_access_key = "jyQQ3BcRl073rtbb9GnA"
minio_secret_key = "2kVRJbQUIVFn0VTBZFsfEW09XUyFs9rDW6vHj6nB"
minio_bucket_name = "tp1"

# Connexion au client Minio
minio_client = Minio(minio_endpoint,
                     access_key=minio_access_key,
                     secret_key=minio_secret_key,
                     secure=False)

# Appels aux fonctions pour récupérer et stocker les données
retrieve_datasets_jan_to_aug_2023(minio_client, minio_bucket_name)
retrieve_latest_dataset(minio_client, minio_bucket_name)

