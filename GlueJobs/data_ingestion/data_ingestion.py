import boto3
from awsglue.utils import getResolvedOptions
import yaml
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from io import BytesIO
from datetime import datetime

class ConfigFileReader:
    
# Read contents of Config File for this utility

    def __init__(self, utility_name, config_s3_location, dataset_name):
        
        self.config_s3_location = config_s3_location
        self.utility_name = utility_name
        self.dataset_name = dataset_name
        # Initialize the S3 client
        self.s3_client = boto3.client('s3')

    def read_yaml_from_s3(self):
        # Extract the bucket name and key from the S3 location
        try:
            # Expecting s3 location to start with s3://
            # Check can be added too
            
            path_parts = self.config_s3_location[5:].split('/', 1)  # Skip "s3://"
            bucket_name = path_parts[0]
            
            # Check can be added to verify if config file exist or not
            
            key = path_parts[1]
            
            # Fetch the YAML file from S3
            response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            file_content = response['Body'].read()
            
            # Load the YAML content into a dictionary
            yaml_data = yaml.safe_load(file_content)
            
            # Return the parsed YAML as a dictionary
            return yaml_data[self.utility_name][self.dataset_name]
        
        except Exception as e:
            # Exceptions can be more holistic
            print(f"An error occurred: {e}")
            raise e
            
class CSVReader:
    
# Read csv files from S3 & write back to S3 in parquet format & also creates/update tables in Glue Catalog
# Single utility for this case, but can be expanded to multiple file formats, file pattern match, etc.

    def __init__(self, source_s3_location, batch_id):
        
        self.source_s3_location = source_s3_location
        self.batch_id = batch_id
        
        # Initialize SparkSession
        self.spark = SparkSession.builder.appName("CSVReader").getOrCreate()

    def read_csv(self):
        try:
            # Read CSV files from source S3 location
            # Can configure option from yaml file
            df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(self.source_s3_location)
            # Append the batch_id column to the DataFrame
            df = df.withColumn("batch_id", lit(self.batch_id))
            
            print(f"Successfully read CSV data and added batch_id {self.batch_id}")
            return df
        except Exception as e:
            print(f"Error occurred during CSV reading: {e}")
            raise e

class GlueTableManager:
    
# Save df as parquet & create Glue table over it

    def __init__(self, database_name, table_name, source_df, dest_s3_location):
        
        self.database_name = database_name
        self.table_name = table_name
        self.source_df = source_df
        self.dest_s3_location = dest_s3_location
        
        # Initialize SparkSession
        self.spark = SparkSession.builder.appName("GlueTableManager").getOrCreate()

    def create_table_from_df(self):
        try:
            # Write the DataFrame to Parquet format in the destination S3 location
            df = self.source_df
            df.write.partitionBy("batch_id").mode("append").parquet(self.dest_s3_location)

            # Create the Glue Catalog table if it does not exist
            self.create_glue_catalog_table()

            # Refresh partitions if the table already exists
            self.refresh_partitions()

            print(f"Successfully created Glue table {self.table_name} in database {self.database_name}")
        except Exception as e:
            print(f"Error occurred while creating Glue catalog table: {e}")
            raise e
    
    def infer_glue_schema(self):
    # Map Spark data types to Glue data types
        type_mapping = {
            "StringType()": "string",
            "IntegerType()": "int",
            "LongType()": "bigint",
            "DoubleType()": "double",
            "FloatType()": "float",
            "BooleanType()": "boolean",
            "TimestampType()": "timestamp",
            "DateType()": "date",
            "BinaryType()": "binary"
        }
        
        columns = ", ".join(
            f"{field.name} {type_mapping.get(str(field.dataType), 'string')}"
            for field in self.source_df.schema.fields if field.name != "batch_id"
        )
        return columns

    def create_glue_catalog_table(self):
        try:
            
            # Create the Glue Catalog table by reading the Parquet files in the destination S3 location
            column_definitions = self.infer_glue_schema()
            
            self.spark.sql(f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS {self.database_name}.{self.table_name} (
                    {column_definitions}
                )
                PARTITIONED BY (batch_id STRING)
                STORED AS PARQUET
                LOCATION '{self.dest_s3_location}'
            """)

            print(f"Glue table {self.table_name} created or already exists in database {self.database_name}")
        except Exception as e:
            print(f"Error occurred while creating Glue catalog table: {e}")
            
    def refresh_partitions(self):
        try:
            # Run the MSCK REPAIR TABLE command to refresh partitions in Glue
            self.spark.sql(f"MSCK REPAIR TABLE {self.database_name}.{self.table_name}")
            print(f"Successfully refreshed partitions for {self.table_name}")
        except Exception as e:
            print(f"Error occurred while refreshing partitions: {e}")
            raise e

class S3Archiver:
    def __init__(self, source_s3_location, batch_id):
        
# Archive S3 raw files
        self.s3 = boto3.client('s3')
        self.source_bucket, self.source_prefix = self.parse_s3_path(source_s3_location)
        self.source_prefix = self.source_prefix.rstrip('/')
        self.batch_id = batch_id
        self.destination_prefix = self.get_archival_prefix()

    def parse_s3_path(self, s3_path):
        path_parts = s3_path[5:].split('/', 1)  # Skip "s3://"
        bucket_name = path_parts[0]
        key = path_parts[1] if len(path_parts) > 1 else ""
        return bucket_name, key
        
    def get_archival_prefix(self):
        parts = self.source_prefix.rsplit('/', 1)
        return f"{parts[0]}/archival/{parts[1]}/batch_id={batch_id}" if len(parts) > 1 else f"archival/{self.source_prefix}/batch_id={batch_id}"

    def move_files(self):
        objects = self.s3.list_objects_v2(Bucket=self.source_bucket, Prefix=self.source_prefix)
        
        if "Contents" in objects:
            for obj in objects["Contents"]:
                source_key = obj["Key"]
                destination_key = f"{self.destination_prefix}/{source_key[len(self.source_prefix):].lstrip('/')}"

                # Copy the object
                self.s3.copy_object(
                    Bucket=self.source_bucket,
                    CopySource={"Bucket": self.source_bucket, "Key": source_key},
                    Key=destination_key
                )

                # Delete the original object
                # For this assignment not deleting the files in raw locatio
                
                # self.s3.delete_object(Bucket=self.source_bucket, Key=source_key)
                print(f"Moved: {source_key} → {destination_key}")
        else:
            print("No files found to move.")

if __name__ == "__main__":
    
    args = getResolvedOptions(sys.argv, ['utility_name', 'config_s3_location', 'dataset_name', 'batch_date'])
    
    utility_name = args['utility_name']
    config_s3_location = args['config_s3_location']
    dataset_name = args['dataset_name']
    batch_date = args['batch_date']
    
    batch_id = datetime.strptime(batch_date, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y%m%d%H%M%S")
    
    dataset_config_reader = ConfigFileReader(utility_name, config_s3_location, dataset_name)
    dataset_configs = dataset_config_reader.read_yaml_from_s3()
    print(dataset_configs)
    
    source_s3_location = dataset_configs['source_s3_location']
    source_df_reader = CSVReader(source_s3_location, batch_id)
    source_df = source_df_reader.read_csv()
    
    database_name = dataset_configs['database_name']
    table_name = dataset_configs['table_name']
    dest_s3_location = dataset_configs['dest_s3_location']
    glue_table_manager = GlueTableManager(database_name, table_name, source_df ,dest_s3_location)
    create_update_table = glue_table_manager.create_table_from_df()

# Archival(with source file deletion) or file level logging is required in case all files are not processed daily
# For this assignment, archiving but not deleting the source file - change made in function code
    archiver = S3Archiver(source_s3_location, batch_id)
    archiver.move_files()