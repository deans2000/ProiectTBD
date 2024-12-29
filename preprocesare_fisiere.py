"""
Script pentru Preprocesarea Datelor Netflix Prize Data

Acest script utilizează Apache Spark pentru a procesa fișierele brute de date din Netflix Prize Data. Datele conțin evaluări ale filmelor oferite de utilizatori. Scriptul procesează datele pentru a crea un format curat, pregătit pentru analize ulterioare sau modele de învățare automată.

Pașii implementați în script:
1. Configurarea Spark:
   - Setăm executabilul Python utilizat de PySpark pentru rularea codului.
   - Configurăm o sesiune Spark cu resursele necesare pentru procesarea datelor.

2. Funcții principale:
   - `parse_line`: Parsează liniile brute din fișierul de intrare. Identifică rândurile care conțin `MovieID` și rândurile cu `UserID`, `Rating` și `Timestamp`.
   - `process_file`: Procesează un fișier de date. Creează un DataFrame structurat cu valorile extrase, gestionează valori lipsă și transformă coloanele pentru a se potrivi tipurilor de date necesare.

3. Prelucrarea datelor:
   - Se citesc fișierele specificate și se creează un DataFrame pentru fiecare fișier.
   - DataFrame-urile sunt combinate într-un singur DataFrame pentru a avea un set complet de date.

4. Salvarea rezultatelor:
   - Datele procesate sunt salvate într-un fișier CSV pentru utilizare ulterioară.

Obiective:
- Curățarea și transformarea datelor brute în date structurate, utilizând Spark pentru eficiență și scalabilitate.
- Crearea unui pipeline care poate procesa fișiere multiple de date brute și le poate combina într-un format unificat.

Setări necesare:
- Asigurarea prezenței fișierului `winutils.exe` pe Windows pentru a evita erorile legate de Hadoop.
- Setarea corectă a variabilei de mediu `HADOOP_HOME` pentru rularea Spark pe Windows.
- Actualizarea căilor către fișierele de intrare și ieșire.
"""


import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# Configurarea Spark pentru executabil Python
os.environ["PYSPARK_PYTHON"] = "C:/Users/deans/AppData/Local/Programs/Python/Python311/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/Users/deans/AppData/Local/Programs/Python/Python311/python.exe"

# Inițializare Spark
spark = SparkSession.builder \
    .appName("Netflix Data Preprocessing") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Funcție pentru parsarea liniilor
def parse_line_with_movie_id(line, current_movie_id):
    if ":" in line:  # Linie ce conține MovieID
        current_movie_id[0] = line.split(":")[0]  # Actualizăm MovieID
        return None  # Nu returnăm nimic pentru liniile cu MovieID
    else:
        try:
            user_id, rating, timestamp = line.split(",")
            return (current_movie_id[0], user_id, float(rating), timestamp)  # Adăugăm MovieID curent
        except Exception as e:
            print(f"Eroare la parsarea liniei: {line} - {e}")
            return None  # Linie invalidă

# Funcție pentru procesarea fiecărui fișier
def process_file(file_path):
    print(f"Procesăm fișierul: {file_path}")
    
    # Inițializăm o listă pentru a păstra MovieID curent
    current_movie_id = [None]
    
    # Încărcăm datele brute în RDD
    raw_rdd = spark.sparkContext.textFile(file_path)
    
    # Parsăm liniile și eliminăm cele invalide
    parsed_rdd = raw_rdd.map(lambda line: parse_line_with_movie_id(line, current_movie_id)) \
                        .filter(lambda x: x is not None)

    # Definim schema pentru DataFrame
    schema = StructType([
        StructField("MovieID", StringType(), True),
        StructField("UserID", StringType(), True),
        StructField("Rating", FloatType(), True),
        StructField("Timestamp", StringType(), True)  # Schimbăm în string
    ])

    # Cream DataFrame-ul cu schema definită
    df = spark.createDataFrame(parsed_rdd, schema=schema)
    
    # Convertim "Timestamp" din string în tipul TimestampType
    df = df.withColumn("Timestamp", col("Timestamp").cast(TimestampType()))
    
    # Afișăm primele 10 rânduri pentru verificare
    df.show(10)
    
    return df

# Lista fișierelor de procesat
file_list = [
    "C:/Users/deans/.cache/kagglehub/datasets/netflix-inc/netflix-prize-data/versions/2/combined_data_1.txt",
    "C:/Users/deans/.cache/kagglehub/datasets/netflix-inc/netflix-prize-data/versions/2/combined_data_2.txt",
    "C:/Users/deans/.cache/kagglehub/datasets/netflix-inc/netflix-prize-data/versions/2/combined_data_3.txt",
    "C:/Users/deans/.cache/kagglehub/datasets/netflix-inc/netflix-prize-data/versions/2/combined_data_4.txt"
]

# Procesăm fiecare fișier din listă și creăm DataFrame-uri
dataframes = [process_file(file) for file in file_list]

# Unim toate DataFrame-urile
combined_df = dataframes[0]
for df in dataframes[1:]:
    combined_df = combined_df.union(df)

# Afișăm schema finală și primele 20 de rânduri
combined_df.printSchema()
combined_df.show(20)

# Salvăm rezultatul final într-un fișier CSV pentru referință
output_path = "C:/Users/deans/OneDrive/Desktop/ProiectTBD/processed_data"
combined_df.write.csv(output_path, header=True, mode="overwrite")

print(f"Datele procesate au fost salvate în: {output_path}")
