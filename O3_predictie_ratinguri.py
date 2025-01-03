from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
import os

# Configurare Spark
os.environ["PYSPARK_PYTHON"] = "C:/Users/vultu/AppData/Local/Programs/Python/Python311/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/Users/vultu/AppData/Local/Programs/Python/Python311/python.exe"

spark = SparkSession.builder \
    .appName("Predicție Ratinguri") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.memory.storageFraction", "0.2") \
    .getOrCreate()

# Calea setului de date în HDFS
hdfs_path = "hdfs://localhost:9000/user/vultu/netflix_processed"

# Citirea datelor din HDFS
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# Eliminarea coloanei Timestamp
df = df.drop("Timestamp")

# Transformarea coloanelor 'MovieID' și 'UserID' în format numeric
movie_indexer = StringIndexer(inputCol="MovieID", outputCol="MovieID_index", handleInvalid="skip")
user_indexer = StringIndexer(inputCol="UserID", outputCol="UserID_index", handleInvalid="skip")

df = movie_indexer.fit(df).transform(df)
df = user_indexer.fit(df).transform(df)

# Selectarea coloanelor relevante
df = df.select("MovieID_index", "UserID_index", "Rating")

# Crearea unui vector de caracteristici
assembler = VectorAssembler(
    inputCols=["MovieID_index", "UserID_index"],
    outputCol="features"
)
df = assembler.transform(df)

# Împărțirea datelor în seturi de antrenament și testare
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

# Antrenarea unui model de regresie liniară
lr = LinearRegression(featuresCol="features", labelCol="Rating")
lr_model = lr.fit(train_data)

# Evaluarea modelului pe setul de testare
predictions = lr_model.transform(test_data)
evaluator = RegressionEvaluator(labelCol="Rating", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)

print(f"Rădăcina mediei pătrate a erorii (RMSE): {rmse}")

# Afișarea câtorva predicții
predictions.select("MovieID_index", "UserID_index", "Rating", "prediction").show(10)

# Oprirea sesiunii Spark
spark.stop()