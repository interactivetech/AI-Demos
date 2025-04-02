# Data-Engineering/Airflow/scripts/hello_world.py
import time

print("Hello World from the Airflow DAG!")
print("Sleeping for 10 seconds...")
time.sleep(10)
print("Done.")
# from pyspark.sql import SparkSession

# if __name__ == "__main__":
#     spark = SparkSession \
#         .builder \
#         .appName("HelloWorldSpark") \
#         .getOrCreate()

#     # Create a simple RDD and collect it
#     rdd = spark.sparkContext.parallelize(["Hello, world"])
#     result = rdd.collect()

#     for line in result:
#         print(line)

#     spark.stop()