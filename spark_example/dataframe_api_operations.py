from pyspark.sql import SparkSession


spark = (SparkSession
    .builder
    .getOrCreate()) 

ev_data_csv_path = "datasets/ev_population_data.csv"

ev_df = spark.read.csv(ev_data_csv_path, header=True, samplingRatio=0.001)

ev_df.show()

spark.stop()


