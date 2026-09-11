from http import client
import boto3
import psycopg2
import pandas as pd  # Import pandas for DataFrame support
import csv
from io import StringIO
import csv
import sqlalchemy
import datetime
import re
import json
import logging
from detect_delimiter import detect
from sqlalchemy import create_engine, DDL
import chardet
import pytz
# import os
from airflow.models import Variable

# Normal call style
# bucket_name = Variable.get("clearlake_raw_bucket")
secret_name = Variable.get("koios_db_secret_name")

# Create a logger object
logger = logging.getLogger(__name__)

# Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL)
logger.setLevel(logging.INFO)


def get_secret(secret_name)-> dict:
        """Gets a secret from Secrets Manager."""
        region_name = 'us-west-2'
        client = boto3.client('secretsmanager', region_name=region_name)
        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            raise e
        secret_value = get_secret_value_response['SecretString']
        return secret_value


def delete_from_rds(secret_name):
        
        # Create a connection to the RDS database

        db = json.loads(get_secret(secret_name))
        # db = json.loads(os.getenv('db'))
        db_user = db['username']
        db_password = db['password']
        db_host = db['host']
        db_name = db['database']
        logging.info(f'RDS DB Host: {db_host}')
        logging.info(f'RDS DB: {db_name}')
        try:
            db_url = f"postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}"
            engine = create_engine(db_url)
            # Delete Statement
            delete_statement = Variable.get("delete_statements_koios")

            # Apply the DDL statement to the engine
            with engine.connect() as connection:
                connection.execute(delete_statement)
            # metadata = sqlalchemy.MetaData(schema=schema_name)

            logging.info("Delete Completed")
        except BaseException as e:
            logger.error(e)
            return {"Error": f"Unable to delete", "Msg": e}
delete_from_rds(secret_name)