import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

class GlueDataFetcher:
    
# Fetch datasets from Glue Catalog as spark dataframes
    def __init__(self, database_name, table_name, load_type):
        self.database_name = database_name
        self.table_name = table_name
        self.load_type = load_type
        self.spark = SparkSession.builder \
            .appName("Glue Data Fetcher") \
            .getOrCreate()
    
    def get_dataframe(self):
        try:
            # print()
            if self.load_type.lower() == 'full':
                query = f"SELECT * FROM {self.database_name}.{self.table_name}"
            elif self.load_type.lower() == 'latest':
                query = f"SELECT * FROM {self.database_name}.{self.table_name} where batch_id = (select max(batch_id) from {self.database_name}.{self.table_name})"
            else:
                query = ''
            
            if query == '':
                raise Exception("Supported Load Type are full & latest only")
            else:
                df = spark.sql(query)
            return df
        
        except Exception as e:
            print(f"Error fetching data from Glue: {str(e)}")
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
            
if __name__ == "__main__":
    # Fetching latest batch id
    event_table_fetcher = GlueDataFetcher("raw", "raw_event", "latest")
    event = event_table_fetcher.get_dataframe()
    
    # Requires user & content table for foreign key constraint - will take full tables for foreign key constraint from staging layer
    
    user_table_fetcher = GlueDataFetcher("stage", "stage_user", "full")
    user_stg = user_table_fetcher.get_dataframe()
    
    content_table_fetcher = GlueDataFetcher("stage", "stage_content", "full")
    content_stg = content_table_fetcher.get_dataframe()
    
    # As done in Raw EDA, DQ for foreign key constraints on user and content table, and data standardization for eventtimestamp & timespent column
    
    # DQ
    event_stg = event.join(user_stg,'deviceid','left_semi')
    event_stg = event_stg.join(content_stg,event_stg['content_id']==content_stg['_id'],'left_semi')

    # Data Standardization
    event_stg = event_stg.select("deviceid","content_id",to_timestamp(from_unixtime(col("eventtimestamp")/1000),"yyyy-MM-dd HH:mm:ss").alias("eventtimestamp"), \
                                round(col("timespent"),2).alias("timespent"),"eventname","batch_id")
    event_stg = event_stg.fillna(0, subset=["timespent"])
    
    # Saving data frame as staged table_name
    dest_s3_location = 's3://shivam-trial-s3/staging/event/'
    glue_table_manager = GlueTableManager('stage', 'stage_event', event_stg ,dest_s3_location)
    create_update_table = glue_table_manager.create_table_from_df()
    
    
    
