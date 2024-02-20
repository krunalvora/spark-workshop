from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .getOrCreate())

ev_data_csv_path = "../datasets/ev_population_data.csv"

# Read from CSV file
ev_df = spark.read.csv(ev_data_csv_path, header=True, samplingRatio=0.001)
print(ev_df.dtypes)

ev_df.show()

ev_df = ev_df.filter(ev_df['Electric Range'] >= 337)

ev_df.show()
print(f"Count of EVs with range >= 337: {ev_df.count()}")

# Write to CSV file
ev_df.write.csv("../tmp/test.csv", mode="overwrite")

spark.stop()
