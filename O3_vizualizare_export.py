from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count
import os

# Configurarea Spark
os.environ["PYSPARK_PYTHON"] = "C:/Users/stangu/AppData/Local/Programs/Python/Python311/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/Users/stangu/AppData/Local/Programs/Python/Python311/python.exe"

spark = SparkSession.builder \
    .appName("Vizualizare și Export") \
    .getOrCreate()

# Citește datele din HDFS
hdfs_path = "hdfs://localhost:9000/user/stangu/netflix_processed"
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# Creăm un rezumat al datelor
summary = df.groupBy("MovieID").agg(
    count("*").alias("Number_of_Movies"),
    avg("Rating").alias("Average_Rating")
)

print("Rezumatul datelor:")
summary.show()

# Exportăm datele într-un format compatibil Power BI
output_path = "hdfs://localhost:9000/user/stangu/summary_data.csv"
summary.write.csv(output_path, mode="overwrite", header=True)
print(f"Datele rezumate au fost salvate în HDFS la {output_path}")

spark.stop()
