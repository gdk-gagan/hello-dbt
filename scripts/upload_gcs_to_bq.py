""" Ingest data into BigQuery datasets from GCS """

""" 
Approach:

Possible table creation methods
1. Create a table using a ddl with an explicit schema 
2. Create a table by inferring the schema (
   'loal_data_from_uri/df()' function can be used to create a new table 
   using a explicit schema (specificed in LoadJogconfig()) or an inferred schema
Since our GCS data has schema inconsistensies, it is better to create an explicit schema 
and use it for future write operations after handling required data tranformations.

Possible ways to read and write 
1. Ways to read parquet data from GCS 
- using pyarrow, from pyarrow import parquet and parquet.read_table(uri)
- pandas, pd.read_parquet() 
2. Before write, we need to handle any schema changes in the read data s.t the input schema matches with our target schema in bigquery 
In this case, it include data type transformations to make it consistent with the bigquery table schemas.
3. Ways to write BQ 
- using LoadJobConfig() and load_data_from_uri() from bigquery python API - ALSO CREATES A TABLE if it doesn't exist
- using LoadJobConfig() and load_data_from_dataframe() API - same as above
- using pandas, to_gbq() function - DOESNOT CREATE A TABLE. TABLE SHOULD EXIST BEFORE THE CALL.

Considerations
- Data type fluctuations - Handled
- Missing fields - TODO
- Extra fields - TODO
- Mixed data types in a field - TODO
- Documentation for renaming fields, splitting or merging columns - REQUIREMENTS
"""
 
"""NOTE: In pandas_gbq's df.to_gbq(destination_table=<>, project_id=<>, chunksize=<>, table_schema=<list of dict>, progress_bar=True) fn
        destination_table is of form <dataset_id>.<table_id> only. 
        If you pass full_name including <project_id>.<dataset_id>.<table_id>, IT CRIES.
        However, in most bigquery APIs, it a table requires full reference including project id.
        Secondly, there is no implicit type casting between df according to the given bigquery schema. 
        The type conversion has to be explicit. 

    Copying dict to another var, using dic2 = dic1.copy() or dic2 = dic(dic1) DOES NOT WORK. Use deepcopy instead.
    Otherwise create a new dictionary and add items instead of copy.

"""

"""
TODO:
- Move configurations to a yaml file.
- Handle missing fields
- Handle extra fields
- Mixed data types in a field
"""

from google.cloud import storage
from google.cloud import bigquery
from google.api_core.page_iterator import HTTPIterator
from typing import List, Dict
from time import time
import pandas as pd

# Steps to setup google cloud environment vars
# 1. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key. Check gcloud config set.
#     export GOOGLE_APPLICATION_CREDENTIALS="/Users/gdk/google-cloud-sdk/dtc-de-2023.json"
# 2. Create a bucket, 'de-week4-data-lake' from the Google Cloud UI console
# 3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
#     export GCP_GCS_BUCKET='de-week4-data-lake'
# 4. Authenticate with google cloud - 
#     gcloud auth application-default login
# Code:
# import os
# bucket_name = os.environ.get("GCP_GCS_BUCKET", "de-week4-data-lake")
# credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

project_id = "dtc-de-2023-398823"
bucket_name = "de-week4-data-lake"
dataset_id = "ny_taxi"

GREEN_TAXI_SCHEMA = [
        {'name' :'VendorID', 'type' :'FLOAT64'},
        {'name' :'lpep_pickup_datetime', 'type' :'DATETIME'},
        {'name' :'lpep_dropoff_datetime', 'type' :'DATETIME'},
        {'name' :'store_and_fwd_flag', 'type' :'STRING'},
        {'name' :'RatecodeID', 'type' :'INT64'},
        {'name' :'PULocationID', 'type' :'INT64'},
        {'name' :'DOLocationID', 'type' :'INT64'},
        {'name' :'passenger_count', 'type' :'INT64'},
        {'name' :'trip_distance', 'type' :'FLOAT64'},
        {'name' :'fare_amount', 'type' :'FLOAT64'},
        {'name' :'extra', 'type' :'FLOAT64'},
        {'name' :'mta_tax', 'type' :'FLOAT64'},
        {'name' :'tip_amount', 'type' :'FLOAT64'},
        {'name' :'tolls_amount', 'type' :'FLOAT64'},
        {'name' :'ehail_fee', 'type' :'FLOAT64'},
        {'name' :'improvement_surcharge', 'type' :'FLOAT64'},
        {'name' :'total_amount', 'type' :'FLOAT64'},
        {'name' :'payment_type', 'type' :'INT64'},
        {'name' :'trip_type', 'type' :'INT64'},
        {'name' :'congestion_surcharge', 'type' :'FLOAT64'}
        ]

YELLOW_TAXI_SCHEMA = [
        {'name':'VendorID', 'type':'FLOAT64'},
        {'name':'tpep_pickup_datetime', 'type':'DATETIME'},
        {'name':'tpep_dropoff_datetime', 'type':'DATETIME'},
        {'name':'passenger_count', 'type':'FLOAT64'},
        {'name':'trip_distance', 'type':'FLOAT64'},
        {'name':'RatecodeID', 'type':'FLOAT64'},
        {'name':'store_and_fwd_flag', 'type':'STRING'},
        {'name':'PULocationID', 'type':'INT64'},
        {'name':'DOLocationID', 'type':'INT64'},
        {'name':'payment_type', 'type':'FLOAT64'},
        {'name':'fare_amount', 'type':'FLOAT64'},
        {'name':'extra', 'type':'FLOAT64'},
        {'name':'mta_tax', 'type':'FLOAT64'},
        {'name':'tip_amount', 'type':'FLOAT64'},
        {'name':'tolls_amount', 'type':'FLOAT64'},
        {'name':'improvement_surcharge', 'type':'FLOAT64'},
        {'name':'total_amount', 'type':'FLOAT64'},
        {'name':'congestion_surcharge', 'type':'FLOAT64'}
        ]

def get_gcs_client() -> storage.Client:
    """Get google cloud storage client from the google SDK.
      
      If google cloud environment variables for credentials and project are not set, pass the values during client creation
      Otherwise, create a client without specific params
      client = storage.Client().from_service_account_json(credentials_path, project=project_id)

    Returns:
        google.cloud.storage.Client: A client object for interacting with Google Cloud Storage.
    """

    try:
        client = storage.Client()
        print("GCS client type", type(client))
    except Exception as e:
        print(f"Failed to get google storage client with exception {e}")
        raise
    return client

def get_bq_client() -> bigquery.Client:
    """Client object for interacting with BigQuery

    If google cloud environment variables for credentials and project are not set, pass the values during client creation
    Otherwise, create a client without specific params
    client = bigquery.Client().from_service_account_json(credentials_path, project=project_id)
    
    Returns:
        bigquery.Client: Bigquery client 
    """
    try:
        client = bigquery.Client()
    except Exception as e:
        print(f"Failed to get bigquery client with exception {e}")
        raise
    return client

def read_from_gcs(gcs_client: storage.Client, bucket_name: str) -> HTTPIterator:
    """ Read all blobs in the given GCS bucket

    Args:
        gcs_client (storage.Client): Google storage client
        bucket_name (str): name of the bucket

    Returns:
        HTTPIterator: returns an iterator to the blobs
    """
    
    blobs = gcs_client.list_blobs(bucket_name)  
    print("blobs type", type(blobs))      
    return blobs

def build_df_dtypes_from_bq_dtypes(bq_schema: List[dict]) -> List[dict]:
    """Map the given bigquery datatypes to pandas datatypes and return a list

    Args:
        bq_schema (List[dict]): List of bigquery fields and corresponding data types.

    Returns:
        List[dict]: List of pandas datatypes for each field
    """
    df_schema = []
    for col in bq_schema:
        if col["type"] == 'FLOAT64':
            dtype = 'float'
        elif col["type"] == 'STRING':
            dtype = 'str'
        elif col["type"] == 'INT64':
            dtype  = 'int'
        elif col["type"] == 'DATETIME':
            dtype  = 'dt'
        else:
            dtype  ='str'
        df_schema.append({'name':col['name'], 'type':dtype})
    
    print("Target df Schema : ", df_schema)
    return df_schema

def cast_df_to_given_dtype(df: pd.DataFrame, df_dtypes: List[dict]) -> pd.DataFrame:
    """
    Type-casts each column of a DataFrame to the given schema.

    Args:
        df (pandas.DataFrame): The DataFrame to be type-casted.
        schema (List[dict]): List of dictionaries containing column names and their corresponding types.

    Returns:
        pandas.DataFrame: The type-casted DataFrame.
    """
    for col in df_dtypes:
        if col["type"]!= 'dt':  
            df[col["name"]] = df[col["name"]].astype(col["type"])
        else:
            df[col["name"]] = pd.to_datetime(df[col["name"]])

    return df

def get_bq_schema_from_config(table_id: str) -> str:
    """Get bigquery schema for the given table from configs

    Args:
        table_id (str): table name only without project and dataset name

    Returns:
        str: big query schema config variable
    """
    if table_id == 'green_taxi':
        schema = GREEN_TAXI_SCHEMA
    elif table_id == 'yellow_taxi':
        schema = YELLOW_TAXI_SCHEMA
    else: 
        schema = ''
    return schema

def get_bq_schema_from_bq_dtype(bq_dtype: List(dict)) -> List[dict]:
    """Format schema for bigquery job config object in the form 
         schema = [bigquery.SchemaField('VendorID', 'FLOAT64'), ... ]

    Args:
        bq_dtype (List[dict]): list of fields and corresponding bigquery data types

    Returns:
        List[dict]: Formatted Bigquery schema as SchemaField object
    """
    
    bq_schema = []
    for col in bq_dtype:
        bq_schema.append(bigquery.SchemaField(col['name'], field_type=col['type']))

    return bq_schema



def write_blob_to_bq_table(bq_client: bigquery.Client, bucket_name: str, 
                           blob_folder:str, blob_name: str, dataset_id: str , 
                           table_id: str, clean_df:bool) -> None:
    """Write blob to a bigquery table. 
    Creates a new table and appends records if table does not exist.
    If table exists, pass a schema and set clean_df = True

    Args:
        bq_client (bigquery.Client): Bigquery client for write operation
        bucket_name (str): GCS bucket name to construct the URI
        blob_folder (str): Folder name, if any
        blob_name (str): Blob name
        dataset_id (str): Bigquery dataset ID
        table_id (str): Bigquery table ID
        clean_df (bool): Map dataframe to table schema, if table exists
    """

    start = time()      
       
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    bq_schema = get_bq_schema_from_config(table_id) 

    uri = f'gs://{bucket_name}/{blob_folder}/{blob_name}'
    
    # Read parquet data as a dataframe
    df = pd.read_parquet(uri)
    print(f"BEFORE CLEANING DataFrame dtypes : {df.dtypes}")
    
    if clean_df:
        df_dtype = build_df_dtypes_from_bq_dtypes(bq_schema)
        write_df = cast_df_to_given_dtype(df, df_dtype)
        print(f"AFTER CLEANING DataFrame dtypes : {write_df.dtypes}")
    else:
        write_df = df

    # Create table and append rows if exists
    job_config = bigquery.LoadJobConfig()
    # job_config.source_format = bigquery.SourceFormat.PARQUET # specify if reading from uri (csv or parquet)
    job_config.schema = get_bq_schema_from_bq_dtype(bq_schema)
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND  # Set append behavior, WRITE_TRUNCATE for truncate

    load_job = bq_client.load_table_from_dataframe(
        write_df,
        table_ref,
        job_config=job_config
    )

    load_job.result()
    end = time()
    print(f'Appended {load_job.output_rows} rows to {dataset_id}.{table_id}.')
    print(f'Took {end - start} seconds.')

# def write_blob_to_bq_table_pandas_gbq(bq_client, bucket_name, blob_folder, blob_name, dataset_id):
#     """Write using to_gbq pandas function"""
#     start = time()      
#     table_id = f"{blob_folder}_taxi"
    
#     dataset_ref = bq_client.dataset(dataset_id)
#     table_ref = f"{dataset_id}.{table_id}"

#     uri = f'gs://{bucket_name}/{blob_folder}/{blob_name}'
    
#     # Read parquet data as a dataframe
#     df = pd.read_parquet(uri)
#     print(f"BEFORE CLEANING Data frame dtypes : {df.dtypes}")
#     bq_dtype = get_bq_schema_from_bq_dtype(table_id)
#     df_dtype = build_df_dtypes_from_bq_dtypes(bq_dtype)
#     write_df = cast_df_to_given_dtype(df, df_dtype)
#     print(f"AFTER CLEANING Data frame dtypes : {write_df.dtypes}")
    
#     write_df.to_gbq(destination_table=table_ref, project_id=project_id, if_exists='append', table_schema=bq_dtype, progress_bar=True)
#     end = time()
#     print(f'Appended {len(df)} rows to {dataset_id}.{table_id}.')
#     print(f'Took {end - start} seconds.')

# def write_to_bigquery(blobs, bq_client, bucket_name, dataset_id):
#     for blob in blobs:
#         blob_split = blob.id.split("/")
#         blob_folder = blob_split[1]
#         blob_name = blob_split[2]
#         if blob_folder == 'fhv':
#             continue
#         else:
#             print(f"Ingesting {blob_name} ... ")
#             write_blob_to_bq_table(bq_client, bucket_name, blob_folder, blob_name, dataset_id)

if __name__ == '__main__':
    gcs_client = get_gcs_client()
    bq_client = get_bq_client()
    blobs = read_from_gcs(gcs_client, bucket_name)

    for blob in blobs:
        blob_split = blob.id.split("/")
        blob_folder = blob_split[1]
        blob_name = blob_split[2]
        table_id = f"{blob_folder}_taxi_test"
        if blob_folder == 'fhv':
            continue
        else:
            print(f"Ingesting {blob_name} ... ")
            
            # Since this is an ELT workflow, we'll create new tables with raw data and use dbt for type and other transformations.
            write_blob_to_bq_table(bq_client, bucket_name, blob_folder, blob_name, dataset_id, table_id, clean_df=False)

    # write_to_bigquery(data, bq_client, bucket_name, dataset_id)