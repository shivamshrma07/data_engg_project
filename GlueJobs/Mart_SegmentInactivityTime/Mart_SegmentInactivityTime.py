import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.job import Job
from datetime import datetime
  
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
    
    # Fetching full data of user, content & event tables
    
    user_table_fetcher = GlueDataFetcher("stage", "stage_user", "full")
    user_df = user_table_fetcher.get_dataframe()
    
    content_table_fetcher = GlueDataFetcher("stage", "stage_content", "full")
    content_df = content_table_fetcher.get_dataframe()
    
    event_table_fetcher = GlueDataFetcher("stage", "stage_event", "full")
    event_df = event_table_fetcher.get_dataframe()
    
    # User Segmentation Table
    
    user_segment_table_fetcher = GlueDataFetcher("mart", "mart_user_segmentation", "full")
    segmented_df = user_segment_table_fetcher.get_dataframe()
    
    # Remove events which are only 'notifications-shown'
    event_df_not_shown = event_df.filter(event_df.eventname != 'Shown')
    
    # Recent date when user interacted with App
    aggregated_events = event_df_not_shown.withColumn("event_date", to_date(event_df_not_shown.eventtimestamp,'yyyy-MM-dd')).groupBy("deviceid").agg(max("event_date").alias("last_event_date"))
    # Installation date for given device
    segmented_with_date_df = segmented_df.join(user_df.select("deviceid", "install_dt"), "deviceid", "left").join(aggregated_events, "deviceid", "left")
    # Last interaction date and install date difference
    segmented_with_date_df = segmented_with_date_df.withColumn("days_since_install", datediff(segmented_with_date_df.last_event_date, segmented_with_date_df.install_dt))

    # There are some vlaues (18) where days_since_install is negative - removing them as it is data issue
    segmented_with_date_df = segmented_with_date_df.filter(segmented_with_date_df.days_since_install >= 0)
    
    # Add inactive user period for day1, day2, week1, month1, month3
    inactive_user_df = segmented_with_date_df.withColumn("inactive_user_period", \
        when(col("days_since_install") == 0, "D1") \
        .when(col("days_since_install") == 1, "D2") \
        .when(col("days_since_install") < 7, "W1") \
        .when(col("days_since_install") < 30, "M1") \
        .when(col("days_since_install") < 90, "M3") \
        .otherwise("Other") \
    )
    
    # User count for user-segment & above periods
    inactive_user_summary_df = inactive_user_df.groupBy("user_segment", "inactive_user_period").agg(countDistinct("deviceid").alias("inactive_user_count"))
    
    # Total users in each user-segment
    total_user_segment_df = segmented_df.groupBy("user_segment").agg(countDistinct("deviceid").alias("total_users"))
    
    # Get percentage of inactive users for each user-segment & above periods
    inactive_user_agg_df = inactive_user_summary_df.join(total_user_segment_df, "user_segment", "left").withColumn("inactive_percentage", round((col("inactive_user_count") / col("total_users"))*100,2))
    
    # Pivot above dataframe for different periods on user-segments
    inactive_user_pivot_df = inactive_user_agg_df.groupBy("user_segment").pivot("inactive_user_period").agg(first("inactive_percentage"))
    segment_inactivity_time_metrics_df = inactive_user_pivot_df.na.fill(0)
    
    # Saving data frame as table in mart layer
    args = getResolvedOptions(sys.argv, ['batch_date'])
    batch_date = args['batch_date']
    
    batch_id = datetime.strptime(batch_date, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y%m%d%H%M%S")
    
    segment_inactivity_time_metrics_df = segment_inactivity_time_metrics_df.withColumn('batch_id',lit(batch_id))
    dest_s3_location = 's3://shivam-trial-s3/mart/segment_inactivity_time_metrics/'
    glue_table_manager = GlueTableManager('mart', 'mart_segment_inactivity_time_metrics', segment_inactivity_time_metrics_df ,dest_s3_location)
    create_update_table = glue_table_manager.create_table_from_df()
    
    
    
