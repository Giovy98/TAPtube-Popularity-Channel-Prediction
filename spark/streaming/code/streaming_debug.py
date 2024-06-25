from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import types as tp
from pyspark.sql.functions import from_json, col, when, to_json, struct
from elasticsearch import Elasticsearch
import json

APP_NAME = 'reddit-streaming-class-prediction'
APP_BATCH_INTERVAL = 1

elastic_host = "https://es01:9200"
elastic_index = "taptube_channel_debug"

# Elasticsearch configuration
es = Elasticsearch(
    elastic_host,
    ca_certs="/app/certs/ca/ca.crt",
    basic_auth=("elastic", "passwordTAP"),
)

def get_record_schema():
    """
    Define the schema of the incoming records.
    """
    return tp.StructType([
        tp.StructField("ChannelID:", tp.StringType(), False),
        tp.StructField("Title", tp.StringType(), False),
        tp.StructField("Country", tp.StringType(), False),
        tp.StructField("Subscribers", tp.IntegerType(), False),
        tp.StructField("TotalVideo", tp.IntegerType(), False),
        tp.StructField("Views", tp.IntegerType(), False),
        tp.StructField("Join_date", tp.StringType(), False),  # Changed to StringType
        tp.StructField("@timestamp", tp.StringType(), False)   # Changed to StringType
    ])

def process_batch(batch_df, batch_id):
    """
    Kafka ->  filter: Json  -> ElasticSearch
    """
    for idx, row in enumerate(batch_df.collect()):
        row_dict = row.asDict()
        id = f'{batch_id}-{idx}'
        try:
            row_json = json.dumps(row_dict)  # Ensure row is JSON serializable
            resp = es.index(index=elastic_index, id=id, body=row_json)
            print(f"Indexed record {id} to Elasticsearch with response: {resp['result']}")
        except Exception as e:
            print(f"Error indexing record {id} to Elasticsearch: {str(e)}")

def main():
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    print("[Read]: Model Loaded")

    model = PipelineModel.load("model")  # Model loaded
    schema = get_record_schema()

    df = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'broker:9092') \
        .option('subscribe', 'taptube_channel') \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .selectExpr("data.*")
    
    print("[Use]: Model Loaded")
    
    prediction = model.transform(df)
    
    prediction = prediction.withColumn("popolarita",
                    when(prediction["prediction"] == 0, "molto popolare") \
                    .when(prediction["prediction"] == 1, "poco popolare") \
                    .when(prediction["prediction"] == 2, "non popolare") \
                    .otherwise(prediction["prediction"]))
    
    prediction = prediction.select("ChannelID:", "Title", "Country", "Subscribers", "TotalVideo", "Views", "Join_date", "@timestamp", "popolarita")
    
    # Convert the necessary fields to JSON serializable format
    prediction = prediction.withColumn("Join_date", col("Join_date").cast("string"))
    prediction = prediction.withColumn("@timestamp", col("@timestamp").cast("string"))
    
    prediction.writeStream\
        .foreachBatch(process_batch) \
        .start() \
        .awaitTermination()

if __name__ == '__main__': 
    main()
