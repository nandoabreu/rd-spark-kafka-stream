import json

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def load_config(config_file: str) -> dict:
    with open(config_file) as f:
        return json.load(f)


class Streaming_ETL:
    def __init__(self, config: dict):
        self.spark_session_info = config['spark_session_info']
        self.kafka_information = config['kafka_information']
        self.database_info = config['database_info']

        # Initialize SparkSession
        self.spark = SparkSession.builder \
            .appName("Streaming_Devices_Data") \
            .config("spark.jars", self.spark_session_info['postgres_jars_path']) \
            .config("spark.jars.packages", self.spark_session_info['jars_package']) \
            .getOrCreate()


    def extract(self) -> DataFrame:
        # Read Kafka Stream and extract data schema
        input_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_information['host']) \
            .option("subscribe", self.kafka_information['topic']) \
            .load()

        return input_df

    def process(self, input_df: DataFrame) -> DataFrame:
        # Select json values to further processing and normalization
        processed_df = input_df.select(
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("json_value")
            )
        
        processed_df = processed_df.fillna(0)
        )

        return processed_df.fillna(0)

        return processed_df

   
    def Transform_erreduarte_device(self, processed_df):
        
        #Select and normalize data from device1 = erreduarte      
        transformed_df_key_1 = processed_df.select(
                                "key",
                                F.expr("get_json_object(json_value, '$.device')").cast("string").alias("device"),
                                F.expr("get_json_object(json_value, '$.collected_at')").cast("timestamp").alias("collected_at"),
                                F.expr("get_json_object(json_value, '$.cpu.thermal_zones[0]')").cast("float").alias("thermal_zones_0"),
                                F.expr("get_json_object(json_value, '$.cpu.thermal_zones[1]')").cast("float").alias("thermal_zones_1"),
                                F.expr("get_json_object(json_value, '$.cpu.thermal_zones[2]')").cast("float").alias("thermal_zones_2"),
                                F.expr("get_json_object(json_value, '$.cpu.thermal_zones[3]')").cast("float").alias("thermal_zones_3"),
                                F.expr("get_json_object(json_value, '$.cpu.thermal_zones[4]')").cast("float").alias("thermal_zones_4"),
                                F.expr("get_json_object(json_value, '$.cpu.thermal_zones[5]')").cast("float").alias("thermal_zones_5"),
                                F.expr("get_json_object(json_value, '$.gpu.nvidia')").cast("float").alias("gpu_temp")
                                ).where("key == 'erreduarte'")
        
        #Apply aggregations
        df_erreduarte = transformed_df_key_1.select(
                                F.col("key"),
                                F.col("device"),
                                F.col("collected_at"),
                                ((F.col("thermal_zones_0") + F.col("thermal_zones_1") + F.col("thermal_zones_2") + F.col("thermal_zones_3") + F.col("thermal_zones_4") + 
                                F.col("thermal_zones_5"))/6).alias("cpu_temp"),
                                F.col("gpu_temp")
                    )

        return df_erreduarte
    
    
    def Transform_3tplap_device(self, processed_df):

        #Select and normalize data from device2 = 3tplap
        transformed_df_key_2 = processed_df.select(
                                "key",
                                F.expr("get_json_object(json_value, '$.device')").cast("string").alias("device"),
                                F.expr("get_json_object(json_value, '$.collected_at')").cast("timestamp").alias("collected_at"),
                                F.expr("get_json_object(json_value, '$.cpu.sensors.Tctl')").cast("float").alias("cpu_0"),
                                F.expr("get_json_object(json_value, '$.cpu.sensors.CPU')").cast("float").alias("cpu_1"),
                                F.expr("get_json_object(json_value, '$.gpu.sensors.GPU')").cast("float").alias("GPU_0"),
                                F.expr("get_json_object(json_value, '$.gpu.sensors.edge')").cast("float").alias("GPU_edge")
                                ).where("key == '3tplap'")
        #Apply aggregations
        df_3tplap = transformed_df_key_2.select(
                                F.col("key"),
                                F.col("device"),
                                F.col("collected_at"),
                                ((F.col("cpu_0") + F.col("cpu_1"))/2).alias("cpu_temp"),
                                ((F.col("GPU_0") + F.col("GPU_edge"))/2).alias("gpu_temp")
                                )
        return df_3tplap
    
    
    
    def merge_data(self, df_erreduarte, df_3tplap):

        #Merge both normalized DFs into a single one
        merged_df = df_erreduarte.union(df_3tplap)

        return merged_df

    def write_to_postgres(self, batch_df, _):
        # Set config to write transformed incoming data
        batch_df.write \
            .format("jdbc") \
            .option("url", self.database_info['db_url']) \
            .option("dbtable", self.database_info['table']) \
            .option("user", self.database_info['user']) \
            .option("password", self.database_info['password']) \
            .option("driver", self.database_info['driver']) \
            .mode(self.database_info['mode']) \
            .save()

    def start_streaming(self):

        input_df = self.Extract()
        processed_df = self.Process(input_df)
        df_erreduarte = self.Transform_erreduarte_device(processed_df)
        df_3tplap = self.Transform_3tplap_device(processed_df)
        merged_df = self.merge_data(df_erreduarte, df_3tplap)

        #Write transformed dataframe (df_final) to Postgres   
        streamingQuery = merged_df.writeStream\
                .foreachBatch(self.write_to_postgres)\
                .outputMode("append")\
                .start()

        streamingQuery.awaitTermination()


if __name__ == "__main__":
    config_filepath = 'config.json'

    config = load_config(config_file=config_filepath)
    if not config:
        raise ValueError(f'Config from {config_filepath} not fetched or parsed')

    process_data = Streaming_ETL(config=config)
    process_data.start_streaming()
