from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg
import os

# Configurarea Spark
os.environ["PYSPARK_PYTHON"] = "C:/Users/deans/AppData/Local/Programs/Python/Python311/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/Users/deans/AppData/Local/Programs/Python/Python311/python.exe"

spark = SparkSession.builder \
    .appName("Analiza Exploratorie a Datelor") \
    .getOrCreate()

# Citește datele din HDFS
hdfs_path = "hdfs://localhost:9000/user/deans/netflix_processed"
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# Distribuția ratingurilor
print("Distribuția ratingurilor:")
df.groupBy("Rating").count().orderBy(col("Rating").asc()).show()

# Numărul de recenzii per film
print("Numărul de recenzii per film:")
df.groupBy("MovieID").count().orderBy(col("count").desc()).show(10)  # Top 10 filme cu cele mai multe recenzii

# Numărul total de utilizatori activi
num_users = df.select("UserID").distinct().count()
print(f"Numărul total de utilizatori activi în sistem: {num_users}")

# Oprirea sesiunii Spark
spark.stop()