from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql import types as tp
from pyspark.sql.functions import from_json, col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

APP_NAME = 'yt_channel_clustering'
dataset_path = './dataset/yt_dataset_latest_withdate.csv'

def get_streaming_schema():
    return tp.StructType([
        tp.StructField("ChannelID:", tp.StringType(), False),
        tp.StructField("Title", tp.StringType(), False),
        tp.StructField("Country", tp.StringType(), False),
        tp.StructField("Subscribers", tp.IntegerType(), False),
        tp.StructField("TotalVideo", tp.IntegerType(), False),
        tp.StructField("Views", tp.IntegerType(), False),
        tp.StructField("Join_date", tp.DateType(), False),
        tp.StructField("@timestamp", tp.TimestampType(), False)
    ])

def train_model(spark):
    print("Training model...")
    df = spark.read.csv(dataset_path,
                        header=True,
                        inferSchema=True,
                        sep=',')
    
    assembler = VectorAssembler(inputCols=['Subscribers', 'Views'], outputCol='features', handleInvalid="skip")
    kmeans = KMeans(featuresCol="features", k=3)
    pipeline = Pipeline(stages=[assembler, kmeans])
    
    model = pipeline.fit(df)
    print("Model trained.")
    
    return model

def process_batch(batch_df, batch_id, model):
    print(f"Processing batch ID: {batch_id}")
    
    # Making predictions using the loaded model
    prediction = model.transform(batch_df)
    
    # Convert necessary fields to JSON serializable format
    prediction = prediction.withColumn("Join_date", col("Join_date").cast("string"))
    prediction = prediction.withColumn("@timestamp", col("@timestamp").cast("string"))
    
    # Selecting relevant columns
    prediction = prediction.select("ChannelID:", "Title", "Country", "Subscribers", "TotalVideo", "Views", "Join_date", "@timestamp", "prediction")
    
    prediction.show()

def main():
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Train the model
    model = train_model(spark)
    
    # Define streaming schema
    schema = get_streaming_schema()
    
    # Read from Kafka
    df = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'broker:9092') \
        .option('subscribe', 'taptube_channel') \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .selectExpr("data.*")
    
    # Process each batch
    df.writeStream \
        .foreachBatch(lambda batch_df, batch_id: process_batch(batch_df, batch_id, model)) \
        .start() \
        .awaitTermination()

if __name__ == "__main__":
    main()
