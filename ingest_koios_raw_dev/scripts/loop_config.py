import boto3
import re
import pytz
from datetime import datetime, timedelta
from S3_To_RDS import LoadS3ToRDS
import json
import logging
import os
from levenshtein import get_closest_matching_item

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

# Create a logger object
logger = logging.getLogger(__name__)
from airflow.models import Variable

# Normal call style
bucket_name = Variable.get("clearlake_raw_bucket")
secret_name = Variable.get("koios_db_secret_name_dev")

s3_prefix = 'clinical/'

# Initialize the S3 client
s3_client = boto3.client('s3')
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)

# List of different date format patterns to match
date_patterns = [
    r'\d{2}[A-Z]{3}\d{4}',      # Matches date like "24SEP2023"
    r'\d{4}-\d{2}-\d{2}',       # Matches date like "2023-09-24"
    r'[A-Z]{3}\d{2}\d{2}',      # Matches date like "JUL0319"
]


# Load the JSON configuration from the file
# with open('303_Ingestion_Config.json', 'r') as json_file:
#     config_data = json.load(json_file)

# if config doesn't work from the airflow variables then uncomment the below
# with open('/usr/local/airflow/dags/scripts/drugdevelopment/ingest_koios_manual/DaPlex_Ingestion_Config.json', 'r') as json_file:
#     config_data = json.load(json_file)

config_data = Variable.get("koios_config_data")
config_data = json.loads(config_data)

# Define the PST timezone
pst_timezone = pytz.timezone('US/Pacific')

# Get the current date in PST timezone
current_date_pst = datetime.now(pst_timezone).date()

# Calculate the start and end of the current day in PST
start_of_day_pst = pst_timezone.localize(datetime(current_date_pst.year, current_date_pst.month, current_date_pst.day))
logging.info(f"start of day in pst: {start_of_day_pst}")
end_of_day_pst = start_of_day_pst + timedelta(days=1)

current_date_pst = datetime.now(pst_timezone).date()
logging.info(f"current date in pst: {current_date_pst}")
yesterday_date_pst = current_date_pst - timedelta(days=1)
logging.info(f"yesterday date in pst: {yesterday_date_pst}")
# Calculate the start and end of yesterday in PST
start_of_yesterday_pst = pst_timezone.localize(datetime(yesterday_date_pst.year, yesterday_date_pst.month, yesterday_date_pst.day))
logging.info(f"start of yesterday pst: {start_of_yesterday_pst}")
end_of_yesterday_pst = start_of_yesterday_pst + timedelta(days=1)


# List objects in the S3 bucket recursively
# def list_objects_recursive(bucket, prefix=None):
#     file_names = []
#     paginator = s3_client.get_paginator('list_objects_v2')
#     for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
#         for obj in page.get('Contents', []):
#             last_modified = obj['LastModified']
#             # Convert last modified timestamp to PST timezone
#             last_modified_pst = last_modified.astimezone(pst_timezone)
#             if start_of_yesterday_pst <= last_modified_pst:
#                 # file_name = obj['Key'].split('/')[-1]
#                 file_name = obj['Key']
#                 file_names.append(file_name)
#                 print(file_name)

#     return file_names

# List objects in the S3 bucket recursively
def list_objects_recursive(bucket, prefix=None):
    file_names = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            last_modified = obj['LastModified']
            # Convert last modified timestamp to PST timezone
            last_modified_pst = last_modified.astimezone(pst_timezone)
            # if start_of_yesterday_pst <= last_modified_pst < end_of_yesterday_pst:
            if start_of_yesterday_pst <= last_modified_pst:
                # file_name = obj['Key'].split('/')[-1]
                file_name = obj['Key']
                file_names.append(file_name)
    print(file_names)
    return file_names, last_modified_pst

def remove_dates_from_filename(filename):
    # Define a regular expression pattern to match various date formats
    date_pattern = r'\d{2,4}(?:-\d{2}-\d{2,4})?|[A-Za-z]{3}\d{2,4}'

    # Use regex to find and remove date formats from the filename
    cleaned_filename = re.sub(date_pattern, '', filename)

    # Remove any extra underscores left after removing dates
    cleaned_filename = "_".join(filter(None, cleaned_filename.split("_")))

    return cleaned_filename


def process_table(matching_items, last_modified_pst):
    """
    Process Each table in the Matching Items
    """
    schema_name = 'koios_raw'

    read_s3 = LoadS3ToRDS(bucket_name)
    if len(matching_items) > 1:
        logging.info(f"Multiple Matches Found: {matching_items}")
    for matching_item in matching_items:
        logging.info(f'Processing Config {matching_item}')
        table_name = matching_item['table_name']
        s3_key = matching_item['s3_key']
        delimiter = matching_item['delimiter']
        if matching_item['delimiter'] == 'Pipe':
            delimiter = '|'
        elif matching_item['delimiter'] == 'Comma':
            delimiter = ','
        elif matching_item['delimiter'] == 'Excel':
            logging.info(f"Processing Excel: {s3_key}")
            logging.info(f'Loading data from file: {s3_key} /n Loading data to table: {table_name}')
            study_id = matching_item.get('studyid')
        read_s3.load_data_to_rds(secret_name,s3_key,table_name,
                                            delimiter=delimiter,schema_name=schema_name, last_modified_pst=last_modified_pst, study_id = study_id)
        # read_s3.load_data_to_rds(secret_name,s3_key,table_name,
        #                                     delimiter=delimiter,schema_name=schema_name, last_modified_pst=last_modified_pst)
        print (f"Loading completed for {table_name}")


def get_closest_matching_pattern(s3_obj,matching_config):
    matching_patterns = []
    logging.info(f"Multiple Matches: {matching_config}")
    for item in matching_config:
        matching_patterns.append(item['file_pattern'])
    closest_matching_config = []
    closest_matching_pattern = get_closest_matching_item(s3_obj, matching_patterns)
    if closest_matching_pattern is not None:
        logging.info(f'Closest Matching File Pattern {closest_matching_pattern}')
        for item in matching_config:
            if item['file_pattern'] == closest_matching_pattern:
                closest_matching_config.append(item)
        return closest_matching_config
    else:
        logging.warn('No matching item found.')
        return matching_config


def get_config(s3_obj):
    matching_config = []
    for config in config_data:
        if config['file_pattern'] in s3_obj and config['studyid'] in s3_obj:
            matching_config.append({"table_name" :config['global_filename'], 
                                    "s3_key":s3_obj, 
                                    "studyid":config['studyid'],
                                    "file_pattern" : config['file_pattern'],
                                    "delimiter" : config['Delimiter']})
        elif config['file_pattern'] in s3_obj and config['studyid'] == 'ALL':
            matching_config.append({"table_name" :config['global_filename'],
                                    "s3_key":s3_obj,
                                    "studyid": config['studyid'],
                                    "file_pattern" : config['file_pattern'],
                                    "delimiter" : config['Delimiter']})
    if len(matching_config) > 1:
        matching_config = get_closest_matching_pattern(s3_obj,matching_config)
    return matching_config


def process_event():
    skipped_Files = []
    data_config = []

    # Retrieve all objects in the S3 bucket
    all_objects, last_modified_pst = list_objects_recursive(bucket_name,s3_prefix)
    for object in all_objects:
        data_config = get_config(object)
        if len(data_config) > 0:
            # logging.info(f"Loading Config {data_config}")
            process_table(data_config, last_modified_pst)
        else:
            logging.critical(f"Did not find a config for {object}")
            skipped_Files.append(object)

    print ("Skipped FIles Due To No Config",json.dumps(skipped_Files,indent=4))


if __name__ == '__main__':
    process_event()