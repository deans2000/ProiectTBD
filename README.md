# Proiect Big Data: Analiza Datelor Netflix Prize

Acest proiect implementează un scenariu Big Data utilizând setul de date **Netflix Prize**. Scopul proiectului este achiziția, stocarea și analiza datelor, utilizând tehnologii precum Hadoop și Apache Spark.

---

## Set de Date Utilizat
Setul de date **Netflix Prize** conține evaluări ale filmelor oferite de utilizatori. Este utilizat pentru a demonstra utilizarea tehnologiilor Big Data în procesarea și analiza datelor. Datele sunt disponibile pe [Kaggle](https://www.kaggle.com/datasets/netflix-inc/netflix-prize-data).

---

## Obiectivele Proiectului

### **Obiectiv 1: Colectarea și Pregătirea Datelor**
- Utilizarea **Apache Spark** pentru procesarea fișierelor brute din setul de date.
- Curățarea și transformarea datelor în format structurat, incluzând:
  - Parsarea liniilor brute pentru a extrage `MovieID`, `UserID`, `Rating` și `Timestamp`.
  - Eliminarea valorilor invalide sau incomplete.
- Salvarea datelor procesate într-un format unificat (CSV) pentru utilizare ulterioară.

---

### **Obiectiv 2: Stocarea Datelor**
- Utilizarea **Hadoop Distributed File System (HDFS)** pentru stocarea datelor procesate.
- Integrarea **Spark SQL** pentru:
  - Crearea tabelelor temporare.
  - Executarea interogărilor pentru a verifica și analiza datele.

---

### **Obiectiv 3: Construcția Scenariilor de Analiză**

#### **Scenariul 1: Analiză Exploratorie**
- Calcularea distribuției ratingurilor.
- Identificarea filmelor cu cel mai mare număr de recenzii.
- Calcularea numărului total de utilizatori activi.

#### **Scenariul 2: Predicția Ratingurilor**
- Antrenarea unui model de **regresie liniară** utilizând Spark ML pentru a prezice ratingurile pe baza:
  - Identificatorului utilizatorului (`UserID`).
  - Identificatorului filmului (`MovieID`).
- Evaluarea modelului folosind metrici precum **Rădăcina Mediei Pătrate a Erorii (RMSE)**.

#### **Scenariul 3: Vizualizare și Export**
- Crearea unui rezumat al datelor, incluzând:
  - Numărul de filme pentru fiecare `MovieID`.
  - Media ratingurilor pentru fiecare film.
- Exportarea datelor rezumate într-un format compatibil pentru vizualizare.

---

## Tehnologii Utilizate
1. **Hadoop**:
   - Stocarea datelor în HDFS.
2. **Apache Spark**:
   - Procesarea, analiza și construirea modelelor de învățare automată.
3. **Python**:
   - Dezvoltarea scripturilor pentru colectarea, procesarea și analiza datelor.

---

## Mod de Utilizare

1. **Configurarea Mediului**:
   - Asigurați-vă că Hadoop și Apache Spark sunt configurate corect.
   - Adăugați fișierele brute în locația specificată pentru procesare.

2. **Rularea Scripturilor**:
   - Executați scripturile în următoarea ordine:
     1. Preprocesarea datelor brute (`preprocesare_fisiere.py`).
     2. Stocarea datelor procesate în HDFS (`O2_store_data_hdfs_spark.py`).
     3. Scenariile de analiză (`O3_analiza_exploratorie.py`, `O3_predictie_ratinguri.py`, `O3_vizualizare_export.py`).

---

## Rezultate

1. **Date Structurate**:
   - Toate fișierele brute sunt procesate, curățate și transformate într-un format structurat.

2. **Stocarea Big Data**:
   - Datele procesate sunt stocate în **HDFS**, pregătite pentru analize ulterioare.

3. **Scenarii de Analiză**:
   - **Explorare**: Statistici relevante despre ratinguri, utilizatori și filme.
   - **Predicții**: Modele de învățare automată care prezic ratinguri bazate pe `UserID` și `MovieID`.

---

Proiect realizat de Slatinaț Dean, Stângu Eduard și Vulturar Bogdan. Pentru detalii suplimentare, consultați codul din fișierele asociate fiecărui obiectiv.
