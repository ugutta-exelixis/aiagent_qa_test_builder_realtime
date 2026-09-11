from http import client
import boto3
import psycopg2
import pandas as pd  # Import pandas for DataFrame support
import csv
from io import StringIO, BytesIO 
import csv
import sqlalchemy
import datetime
import re
import json
import logging
from detect_delimiter import detect
from sqlalchemy import create_engine, DDL, Column
import chardet
import pytz
import urllib.parse
import magic 
# import os

# Create a logger object
logger = logging.getLogger(__name__)

# Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL)
logger.setLevel(logging.INFO)


class LoadS3ToRDS:
    def __init__(self,Bucket) -> None:
        self.Bucket = Bucket
        
    def extract_date_from_filename(filename):
        # Extract the date from the filename using a regular expression
        date_match = re.search(r'(\d{4}\d{2}\d{2})', filename)
        if date_match:
            return date_match.group(0)
        else:
            return None


    def detect_delimiter(self,csv_data):
        dialect = ''
        csv_data = csv_data[1:4096]
        try:
            dialect = detect(csv_data)
            print (f"Dialect, {dialect}")
        except Exception as e:
            logging.error(f"Error retrieving dialect from csv_content: {e}")
        return dialect

    
    def sniffer_detect_delimiter(self,csv_data):
        try:
            sniffer = csv.Sniffer()
            sample = csv_data[1:4096]  # Increase the sample size for more accurate detection
            delimiter = sniffer.sniff(sample).delimiter
            return delimiter
        except csv.Error:
            logging.error("Could not determine delimiter. Using default delimiter (',')...")
            return ','  # Specify a default delimiter
    def detect_file_format(self, file_content: bytes) -> str:
        """Detect file format using python-magic and fallback to delimiter detection if plain text."""
        try:
            mime_type = magic.from_buffer(file_content, mime=True)
            logger.info(f"Detected MIME type by magic: {mime_type}")

            if mime_type == "text/plain":
                encoding_info = chardet.detect(file_content)
                detected_encoding = encoding_info['encoding'] or 'utf-8'
                try:
                    sample_text = file_content.decode(detected_encoding, errors='ignore')[:4096]
                except Exception:
                    sample_text = file_content.decode('utf-8', errors='ignore')[:4096]

                try:
                    delimiter = self.sniffer_detect_delimiter(sample_text)
                    if delimiter in [',', '\t', '|', ';']:
                        logger.info(f"text/plain but delimiter '{delimiter}' detected -> treating as CSV")
                        return "csv"
                    else:
                        logger.info("text/plain with no clear delimiter -> treating as TXT")
                        return "txt"
                except Exception as e:
                    logger.warning(f"Delimiter detection failed for text/plain: {e}")
                    return "txt"

            return mime_type
        except Exception as e:
            logger.error(f"Error detecting file format with magic: {e}")
            return "unknown"

    def load_csv_and_xlsx_to_dataframe(self, last_modified_pst):
        # Download the file from S3
        s3_client = boto3.client('s3')
        # Define the PST timezone
        pst_timezone = pytz.timezone('US/Pacific')
        s3_object = s3_client.get_object(Bucket=self.Bucket, Key=self.key)
        print('Last_Modified date -->',s3_object['LastModified'])
        last_modified_date = s3_object['LastModified'].astimezone(pst_timezone)
        print('Last_Modified date after strftime -->',last_modified_date.strftime('%Y-%m-%d %H:%M:%S'))
        print('Last_Modified date after strftime into pst time -->',last_modified_date)
        # Check the file extension to determine the format (CSV or Excel)
        last_modified_date = pd.to_datetime(last_modified_date.strftime('%Y-%m-%d %H:%M:%S'))
        # last_modified_date = pd.to_datetime(last_modified_date.strftime('%Y-%m-%d %H:%M:%S'))+ pd.Timedelta(minutes=1)
       
        ### read once, wrap in BytesIO
        file_content = s3_object['Body'].read()
        file_like_obj = BytesIO(file_content)
        # Check the file extension to determine the format (CSV or Excel)
        # file_extension = self.key.split('.')[-1].lower()
        file_extension = self.key.split('.')[-1].lower() if '.' in self.key else None

        if not file_extension:
            mime_type = self.detect_file_format(file_content)

            if "csv" in mime_type:
                file_extension = "csv"
            elif "excel" in mime_type:
                file_extension = "xlsx"
            elif "sas" in mime_type:
                file_extension = "sas7bdat"
            elif mime_type == "txt" or mime_type == "text/plain":
                file_extension = "txt"
            else:
                raise ValueError(f"Unsupported file type detected by magic: {mime_type}")

            logger.info(f"File extension inferred from MIME type: {file_extension}")
        else:
            logger.info(f"File extension found in key: {file_extension}")
        if file_extension == 'xlsx':
            # Excel file detected, convert it to CSV
            # excel_data = s3_object['Body'].read()
            xls = pd.ExcelFile(file_like_obj)
            # Assume there is only one sheet, you can modify this if you have multiple sheets
            df = xls.parse(xls.sheet_names[0], header=None)
        elif file_extension=='sas7bdat':
            # s3_url='s3://'+self.Bucket+"/"+self.key
            df= pd.read_sas(file_like_obj,encoding='latin-1')
            # print(f"processing  file {s3_url}")
        elif file_extension == 'csv':
            # CSV file detected, read it directly
            # file_content = s3_object['Body'].read()
            # Detect the encoding of the downloaded content
            encoding_info = chardet.detect(file_content)
            detected_encoding = encoding_info['encoding']
            logger.info(f"The detected encoding is: {detected_encoding}")
            try:
                csv_data = file_content.decode(detected_encoding)
            except UnicodeDecodeError:
                try:
                    fallback_encoding = 'utf-8'
                    csv_data = file_content.decode(fallback_encoding)
                except UnicodeDecodeError:
                    fallback_encoding = 'latin-1'
                    csv_data = file_content.decode(fallback_encoding)
            # Detect the CSV delimiter if not provided
            if not self.delimiter:
                self.delimiter = self.sniffer_detect_delimiter(csv_data)
            logging.info(f'Delimiter: {self.delimiter}')
            df = pd.read_csv(StringIO(csv_data), delimiter=self.delimiter, on_bad_lines='warn')

        else:
            raise ValueError("Unsupported file format. Only CSV and Excel (XLSX) files are supported.")

        # Create a DataFrame from the CSV data

        # Add a 'partition_date' column with the current date
        df['partition_date'] = datetime.datetime.now().strftime('%Y-%m-%d')
        df['last_modified_date'] = last_modified_date

        # Add study_id only if it's missing and the config doesn't say "all"
        study_id_columns = ['study_id', 'studyid', 'study_protocol_number','clinical_study_source_id']
        lower_columns = [col.lower() for col in df.columns]
        
        if not any(col in lower_columns for col in study_id_columns):
            if self.study_id and self.study_id.lower() != 'all':
                file_name = self.key.split('/')[-1]
                match = re.search(r'([A-Z]{2,}\d{3}-\d{3})', file_name)
                study_id_from_filename = match.group(1) if match else 'UNKNOWN'
                df['study_id'] = study_id_from_filename
                logging.info(f"'study_id' column added with value: {study_id_from_filename}")
            else:
                logging.info("No study_id column added because config value is 'all'")
        # if df is None:
        #     print('false')
        # else:
        #     print('true')

        return df        

    
    def get_secret(self,secret_name)-> dict:
        """Gets a secret from Secrets Manager."""
        region_name = 'us-west-2'
        client = boto3.client('secretsmanager', region_name=region_name)
        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            raise e
        secret_value = get_secret_value_response['SecretString']
        return secret_value

    # def alter_table(self, engine, table_name, df):
    #     cursor = connection.cursor()

    #     # Get the existing columns in the table
    #     query = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s"
    #     engine.execute(query, (self.table_name,))
    #     existing_columns = [row[0] for row in cursor.fetchall()]
    #     print(existing_columns)
    #     # Check for new columns in the DataFrame that are not in the table
    #     new_columns = set(df.columns) - set(existing_columns)

    #     if new_columns:
    #         # Generate ALTER TABLE queries to add new columns to the table
    #         for col in new_columns:
    #             data_type = df[col].dtype
                

    #             if data_type == 'int64':
    #                 data_type_str = 'INTEGER'
    #             elif data_type == 'float64':
    #                 data_type_str = 'FLOAT'
    #             elif data_type == 'bool':
    #                 data_type_str = 'BOOLEAN'
    #             else:
    #                 data_type_str = 'VARCHAR(1000)'  # Change the default VARCHAR size as needed

    #             alter_query = f'ALTER TABLE {table_name} ADD COLUMN "{col}" {data_type_str};'
                
    #             engine.execute(alter_query)
    #             print(f"Added column '{col}' to the table.")

    def load_data_to_rds(self,secret_name,s3_key,table_name,delimiter=None,schema_name=None,last_modified_pst=None,study_id = None):
        self.key = s3_key
        self.table_name = table_name.lower()
        self.delimiter = delimiter
        self.last_modified_pst = last_modified_pst
        self.study_id = study_id
        # Create a connection to the RDS database

        db = json.loads(self.get_secret(secret_name))
        # db = json.loads(os.getenv('db'))
        db_user = db['username']
        db_password = db['password']
        db_host = db['host']
        db_name = db['database']
        logging.info(f'RDS DB Host: {db_host}')
        logging.info(f'RDS DB: {db_name}')
        try:
            df = self.load_csv_and_xlsx_to_dataframe(self.last_modified_pst)
            # logging.info(f"Schema Name: {schema_name}")
            logging.info(f"Table Name: {self.table_name}")
            db_url = f"postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}"
            engine = create_engine(db_url)

            # Create a DDL statement to create the schema if it doesn't exist
            create_schema = DDL(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            
            #Convert the column names to lowercase
            df.columns = [col.lower() for col in df.columns]

            # Apply the DDL statement to the engine
            with engine.connect() as connection:
                connection.execute(create_schema)
                #df.to_sql(self.table_name, connection.connection, schema=schema_name, if_exists='append', index=False)
            # metadata = sqlalchemy.MetaData(schema=schema_name)

            #Insert the DataFrame into the RDS table
            df.to_sql(self.table_name, engine, schema=schema_name, if_exists='append', index=False)
            logging.info("df To SQL execution completed")
        except BaseException as e:
            logger.error(e)
            return {"Error": f"Unable to load the table {self.table_name}", "Msg": e}
