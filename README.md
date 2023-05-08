# Intro
There are 2 python files in this solution:
1. `Solution.py` - provides solution for Problems 1-3
2. `Predict.py` - an API service to serve the trained predictive model (Problem 4)
# Solution.py
A bash script starts 10 processes in parallel (to fill up CPU's capacity to about 100%):  
  
`sergei_kachanov spark-3.4.0-bin-hadoop3 $: bash /Volumes/Mac/Code/RiskThinking.AI/Run.sh`  
  
which looks like this:  

```bash
cd /Volumes/Mac/Code/RiskThinking.AI/spark-3.4.0-bin-hadoop3

Solution="/Volumes/Mac/Code/RiskThinking.AI/Solution.py"

Logs="/Volumes/Windows/Win/Code/RiskThinking.AI/Logs"

mkdir -p /Volumes/Windows/Win/Code/RiskThinking.AI/Logs

range="AB"; bin/spark-submit ${Solution} ${range} > ${Logs}/${range}.log 2>&1 &
range="CD"; bin/spark-submit ${Solution} ${range} > ${Logs}/${range}.log 2>&1 &
range="EF"; bin/spark-submit ${Solution} ${range} > ${Logs}/${range}.log 2>&1 &
range="GI"; bin/spark-submit ${Solution} ${range} > ${Logs}/${range}.log 2>&1 &
range="JL"; bin/spark-submit ${Solution} ${range} > ${Logs}/${range}.log 2>&1 &
range="MN"; bin/spark-submit ${Solution} ${range} > ${Logs}/${range}.log 2>&1 &
range="OQ"; bin/spark-submit ${Solution} ${range} > ${Logs}/${range}.log 2>&1 &
range="RS"; bin/spark-submit ${Solution} ${range} > ${Logs}/${range}.log 2>&1 &
range="TU"; bin/spark-submit ${Solution} ${range} > ${Logs}/${range}.log 2>&1 &
range="VZ"; bin/spark-submit ${Solution} ${range} > ${Logs}/${range}.log 2>&1 &
```
or in a more readable form:
```
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py AB <-- 16 mins
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py CD
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py EF
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py GI
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py JL
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py MN
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py OQ
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py RS
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py TU <-- 10 mins
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py VZ
```
It took 16 minutes to process stocks/etfs starting with letters A and B (AB). 'TU' took the least amount of time - 10 minutes. And everything else was in between. Since all ran in parallel - 16 minutes was all it took to process them all.  
  
From previous tests I figured how to group them into 10 groups more or less evenly.  
  
Reading (all the original stocks/etfs) was done from one SSD, writing was done to another SSD.
# Code Outline
I read `symbols_valid_meta.csv` line by line in a loop and for each stock/etf pefrormed the 3 required steps.
# Apache Spark
I used Apache Spark to process data. It required installing Java. The rest could be easily installed via `pip install`
# Output
All output files are stored here - https://drive.google.com/drive/folders/1iTgZZ5kXTOiIKTtWwk39IOYFm3-qYszD?usp=sharing  
Folders Problem 1-3 have been zipped.  
The cpmplete file structure looks like this:
```
RiskThinking.AI
  Logs
    AB <-- logs of bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py AB
    CD
    ..
  Errors
    AB <-- errors of bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py AB
    CD
    ..
  Problem 1
    A
    AA <-- an individual stock/etf
    ..
  Problem 2
    A
    AA <-- an individual stock/etf
    ..
  Problem 3
    A
    AA
      data.pkl <-- sarialized trained model RandomForestRegressor
      error.json <-- mean_absolute_error and mean_squared_error
    ..
```
# Errors
To see the types of errors encountered in the process:
```python
from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = StructType([ StructField('Symbol', StringType(), True),
                      StructField('Error',  StringType(), True)])

err = spark.read.parquet('/Volumes/Windows/Win/Code/RiskThinking.AI/Errors/**', schema=schema)

err.groupBy(err.Error).count().sort(err.Error).show(truncate=False)
```
```
+----------------------------------------------------------------------------------------------------------------------------------------+-----+
|Error                                                                                                                                   |count|
+----------------------------------------------------------------------------------------------------------------------------------------+-----+
|With n_samples=1, test_size=0.2 and train_size=None, the resulting train set will be empty. Adjust any of the aforementioned parameters.|2    |
|[PATH_NOT_FOUND] Path does not exist: file:/Volumes/Mac/Code/RiskThinking.AI/stock-market-dataset/archive/stocks/AGM$A.csv.             |1    |
|[PATH_NOT_FOUND] Path does not exist: file:/Volumes/Mac/Code/RiskThinking.AI/stock-market-dataset/archive/stocks/CARR.V.csv.            |1    |
|[PATH_NOT_FOUND] Path does not exist: file:/Volumes/Mac/Code/RiskThinking.AI/stock-market-dataset/archive/stocks/UTX.V.csv.             |1    |
+----------------------------------------------------------------------------------------------------------------------------------------+-----+
```
# API service - Predict.py
- This is where the service is hosted:  
`http://projecticeman.pythonanywhere.com/predict?Symbol=TSLA&vol_moving_avg=6.601057E6&adj_close_rolling_med=252.994334`
- Flask, which is a web application framework, was used to host the API
- GET request to this service may look like this:  
`http://127.0.0.1:5000/predict?Symbol=COST&vol_moving_avg=1.312480E6&adj_close_rolling_med=6.812650`
- It accepted 3 parameters: Symbol, vol_moving_avg, adj_close_rolling_med
- Folder /Problem 3/ uncompressed is 174 GB. Where I host the API has disk space limit of 512 MB. Therefore I only serve 3: Apple (AAPL), Costco (COST) and Tesla (TSLA). I can manually add some mere spesific ones that may be of inrest, if disk space permits
- The output was a HTML page that looked like this:
![image](https://user-images.githubusercontent.com/124945757/236733573-af8d3f89-2c1d-4e35-b0f0-2d486a27a182.png)
  
A cleaer look of the API service result:  
![image](https://user-images.githubusercontent.com/124945757/236739828-2c4229c4-8c6b-40eb-b24f-ecd0c8df7e1c.png)

