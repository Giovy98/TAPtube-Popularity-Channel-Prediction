from pyspark.ml import PipelineModel
import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import pyspark.sql.types as tp
from pyspark.conf import SparkConf
from pyspark.sql.functions import from_json, col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import pandas as pd
from pyspark.ml import Pipeline # pipeline to transform data
from pyspark.ml.evaluation import ClusteringEvaluator
import time

APP_NAME = 'yt_channel_clustering_model_training'
dataset_path = './dataset/yt_dataset_latest_withdate.csv'
basePath = '/app'



def main():
    
    print("__________trainer__________")
    # Inizializzare una sessione Spark
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    
    # Kafka message structure and Train message structure
    schema_training = tp.StructType([
        tp.StructField("ChannelID:", tp.StringType(), False),
        tp.StructField("Title", tp.StringType(), False),
        tp.StructField("Country", tp.StringType(), False),
        tp.StructField("Subscribers", tp.IntegerType(), False),
        tp.StructField("TotalVideo", tp.IntegerType(), False),
        tp.StructField("Views", tp.IntegerType(), False),
        tp.StructField("Join_date", tp.DateType(), False)
    ])
    
    # Funziona
    print("Reading training set...")
    df = spark.read.csv(dataset_path,
                        schema=schema_training,
                        header=True,
                        sep=',')

    print("Done.")
    
    # Pipeline
    print("Inizio ad Assemblare la Pipeline per il modello...")
    # Assemblare le feature in un vettore
    assembler = VectorAssembler(inputCols=['Subscribers', 'Views'], outputCol='features', handleInvalid = "skip")
    kmeans = KMeans(featuresCol="features", k=3)
    pipeline = Pipeline(stages=[assembler, kmeans])
    print("Finisco di Assemblare la Pipeline per il modello...")

    # Alleno il modello su un dataset di training
    print(".....Inizio Addestramento....")
    model = pipeline.fit(df)
    print(".....Fine Addestramento....")

    # Saving models in local on the mounted volume to be loaded later
    model.write().overwrite().save(basePath + "/model")
    print("Task completed!")

    df.printSchema()
    
    print("Utilizzo modello Pre-allenato")
    
    model_loaded = PipelineModel.load("/app/model")
    
    print("....Valutazione del modello...")
    transformed_df = model_loaded.transform(df)

    transformed_df.groupBy("prediction").count().show()

    # Evaluate clustering by computing Silhouette score
    evaluator = ClusteringEvaluator()

    evaluator.setPredictionCol("prediction")
    silhouette = evaluator.evaluate(transformed_df)
    print("Accuracy = " + str(silhouette))

    print("....Fine Valutazione del modello...")

    
if __name__ == "__main__":
    main()    
    

   
            