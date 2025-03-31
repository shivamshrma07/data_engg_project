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
    
    # Calculate interaction metrics - count of all events for given deviceid
    interaction_metrics = event_df.groupBy("deviceid").pivot("eventname").count().fillna(0)
    interaction_metrics.printSchema()
    
    # Aggregate language preference from content table - language in which content is consumed maximum times from device 
    language_df = event_df.join(content_df.select("_id", "newsLanguage"), event_df.content_id == content_df._id, "left") \
                        .groupBy("deviceid", "newsLanguage").count().alias('count') \
                        .withColumn("rank", row_number().over(Window.partitionBy('deviceid').orderBy(col('count').desc()))) \
                        .filter(col("rank") == 1) \
                        .select("deviceid", col("newsLanguage").alias('preferred_language'))
                        
    # Calculate engagement metrics - No. of total interactions, active days & total time spent on given device 
    event_df_date = event_df.select('deviceid','content_id',to_date('eventtimestamp','yyyy-MM-dd').alias('event_date'),'timespent','eventname','batch_id')
    engagement_df = event_df_date.groupBy("deviceid").agg(count("eventname").alias("total_interactions"), \
                        countDistinct("event_date").alias("active_days"),round(sum("timespent"),2).alias("total_timespent"))
    
    # Engagement dataframe with metrics from interactions, language and engagement dataframe               
    user_engagement_df = interaction_metrics.join(engagement_df, "deviceid", "left").join(language_df, "deviceid", "left")
    
    # User segmentation based on interaction and language patterns
    user_segmentation_df = user_engagement_df.withColumn("user_segment", \
            when((user_engagement_df.total_interactions > 1000) & (user_engagement_df.active_days > 30), concat(lit("Loyal Users - "), initcap(user_engagement_df.preferred_language))) \
            .when(user_engagement_df.Shared > 20, concat(lit("Social Sharers - "), initcap(user_engagement_df.preferred_language)))
            .when((user_engagement_df.Front + user_engagement_df.Back) > 5000, concat(lit("Content Readers - "), initcap(user_engagement_df.preferred_language))) \
            .when(user_engagement_df.Opened > 500, "Notification Engagers") \
            .when(user_engagement_df.total_interactions < 500, concat(lit("Occasional Users - "), initcap(user_engagement_df.preferred_language))) \
            .otherwise("Other"))
            
    # Saving data frame as table in mart layer
    args = getResolvedOptions(sys.argv, ['batch_date'])
    batch_date = args['batch_date']
    
    batch_id = datetime.strptime(batch_date, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y%m%d%H%M%S")
    
    user_segmentation_df = user_segmentation_df.withColumn('batch_id',lit(batch_id))
    dest_s3_location = 's3://shivam-trial-s3/mart/user_segmentation/'
    glue_table_manager = GlueTableManager('mart', 'mart_user_segmentation', user_segmentation_df ,dest_s3_location)
    create_update_table = glue_table_manager.create_table_from_df()
