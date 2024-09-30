from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json



class Streaming_ETL:

    def __init__(self, config_file='config.json'):
        
        with open(config_file, 'r') as f:
            config = json.load(f)

        
        self.spark_session_info = config['spark_session_info']
        self.kafka_information = config['kafka_information']
        self.database_info = config['database_info'] 


        #Initialize SparkSession  
        self.spark = SparkSession.builder\
            .appName("Streaming_Devices_Data")\
            .config("spark.jars", self.spark_session_info['postgres_jars_path'])\
            .config("spark.jars.packages", self.spark_session_info['jars_package'])\
            .getOrCreate()


    def Extract(self):
        #Read kafka stream
        df = self.spark.readStream\
                .format("kafka")\
                .option("kafka.bootstrap.servers", self.kafka_information['host'])\
                .option("subscribe", self.kafka_information['topic'])\
                .load()
        
        return df
    
    def Transform(self, df):
        #Set schema to transform incoming data (previously checked in write in console mode)
        schema = StructType([
                    StructField("device_host", StringType(), True),
                    StructField("queried_at", StringType(), True),
                    StructField("cpu_temperature", FloatType(), True),
                    StructField("gpu_temperature", FloatType(), True),
                                
        ])
        #Select key and values columns
        df = df.select(
            F.col("key").cast("string").alias("key"),  # Access to key message
            F.from_json(F.col("value").cast("string"), schema=schema
            ).alias("json_data") #Access to json content
            
            )
        
        #Tranform json values from 'values'column into dataframe elements        
        df_final = df.select(
            "key",
            "json_data.device_host",
            F.to_timestamp(F.expr("substring(json_data.queried_at, 1, 16)"), "yyyy-MM-dd HH:mm").alias("queried_at"), #Set appropriate timestamp
            "json_data.cpu_temperature",
            "json_data.gpu_temperature",
        
        )

        return df_final 
    
    
       
    def write_to_postgres(self, batch_df, batch_id):
        #Set config to write transformed incoming data   
        batch_df.write\
                .format("jdbc")\
                .option("url", self.database_info['db_url'])\
                .option("dbtable", self.database_info['table'])\
                .option("user", self.database_info['user'])\
                .option("password", self.database_info['password']) \
                .option("driver", self.database_info['driver'])\
                .mode(self.database_info['mode'])\
                    .save()
        

    def start_streaming(self):

        df = self.Extract()
        df_final = self.Transform(df)

        #Write transformed dataframe (df_final) to Postgres   
        streamingQuery = df_final.writeStream\
                .foreachBatch(self.write_to_postgres)\
                .outputMode("append")\
                .start()

        streamingQuery.awaitTermination()


if __name__ == "__main__":
    process_data = Streaming_ETL()
    process_data.start_streaming()
