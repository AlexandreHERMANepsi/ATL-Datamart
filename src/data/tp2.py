import os
from minio import Minio
from minio.error import S3Error
import pandas as pd
from sqlalchemy import create_engine
import io
import pyarrow.parquet as pq

def retrieve_data_from_minio(minio_client, bucket_name):
    dataframes = []
    for obj in minio_client.list_objects(bucket_name, recursive=True):
        filename = obj.object_name
        if filename.endswith('.parquet'):
            try:
                data = minio_client.get_object(bucket_name, filename)
                df = pq.read_table(io.BytesIO(data.read())).to_pandas()
                dataframes.append(df)
            except S3Error as e:
                print(f"Une erreur s'est produite lors de la récupération du fichier {filename} depuis Minio: {e}")
    return pd.concat(dataframes, ignore_index=True)

def load_data_to_postgresql(dataframe, connection_string):
    engine = create_engine(connection_string)
    dataframe.to_sql('trip_data', engine, if_exists='append', index=False)

minio_endpoint = "127.0.0.1:9000"
minio_access_key = "jyQQ3BcRl073rtbb9GnA"
minio_secret_key = "2kVRJbQUIVFn0VTBZFsfEW09XUyFs9rDW6vHj6nB"
minio_bucket_name = "tp1"

minio_client = Minio(minio_endpoint,
                     access_key=minio_access_key,
                     secret_key=minio_secret_key,
                     secure=False)

postgresql_connection_string = 'postgresql://admin:admin@127.0.0.1:5432/trip_database'  # Assurez-vous de remplacer avec vos propres informations de connexion

data = retrieve_data_from_minio(minio_client, minio_bucket_name)

load_data_to_postgresql(data, postgresql_connection_string)

