import os
from pyspark.sql import SparkSession

"""
Script pentru stocarea datelor preprocesate folosind Spark SQL.
Obiective:
1. Configurarea și utilizarea HDFS pentru stocarea datelor procesate.
2. Utilizarea Spark SQL pentru a crea și interoga tabele temporare.
"""

# Configurarea Spark
os.environ["PYSPARK_PYTHON"] = "C:/Users/deans/AppData/Local/Programs/Python/Python311/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/Users/deans/AppData/Local/Programs/Python/Python311/python.exe"

spark = SparkSession.builder \
    .appName("Netflix Data Storage with Spark SQL") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# HDFS Path unde salvăm fișierele
hdfs_path = "hdfs://localhost:9000/user/deans/netflix_processed"

# 1. Citirea datelor preprocesate
input_path = "C:/Users/deans/OneDrive/Desktop/ProiectTBD/processed_data"
df = spark.read.csv(input_path, header=True, inferSchema=True)

# 2. Salvarea în HDFS
def save_to_hdfs(df, hdfs_path):
    print(f"Salvăm datele în HDFS la: {hdfs_path}")
    df.write.csv(hdfs_path, mode="overwrite", header=True)
    print("Salvarea în HDFS s-a realizat cu succes.")

# 3. Crearea și utilizarea tabelelor temporare Spark SQL
def process_with_spark_sql(df):
    print("Creăm tabela temporară Spark SQL...")
    df.createOrReplaceTempView("netflix_data")

    print("Executăm o interogare pe tabela temporară...")
    query_result = spark.sql("SELECT * FROM netflix_data LIMIT 10")
    query_result.show()

# Apelăm funcțiile definite
save_to_hdfs(df, hdfs_path)
process_with_spark_sql(df)

# Oprește sesiunea Spark
spark.stop()

print("Obiectivul 2 este complet!")
