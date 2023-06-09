# Intro
There are 2 python files in this solution:
1. `Solution.py` - provides solution for Problems 1-3
2. `Predict.py` - an API service to serve the trained predictive model (Problem 4)
# Solution.py
A bash script starts 10 processes in parallel (to fill up CPU's capacity to about 100%):  
  
`$: bash /Volumes/Mac/Code/RiskThinking.AI/Run.sh`  
  
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
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py AB <-- 37 mins
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py CD
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py EF
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py GI
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py JL
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py MN
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py OQ
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py RS
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py TU <-- this one
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py VZ <-- or this one are usually the fastest ones and take about 23 mins
```
It took 37 minutes to process stocks/etfs starting with letters A and B (AB). 'TU' or 'VZ' took the least amount of time - 23 minutes. And everything else was in between. Since all ran in parallel - 37 minutes was all it took to process them all.  
  
From previous tests I figured how to group them into 10 groups more or less evenly.  
  
Reading (all the original stocks/etfs) was done from one SSD, writing was done to another SSD.
# Code Outline
I read `symbols_valid_meta.csv` line by line in a loop and for each stock/etf pefrormed the 3 required steps.
# Apache Spark
I used Apache Spark to process data. It required installing Java. The rest could be easily installed via `pip install`
# Output
- All output files are stored here - https://drive.google.com/drive/folders/1iTgZZ5kXTOiIKTtWwk39IOYFm3-qYszD?usp=sharing
- Folders Problem 1-2 have been zipped.
- Folder /Problem 3/ is massive: 164G unzipped (takes more than 1h to zip) - there's only 15 GB available on Google Drive. Therefore I only store some, but not all. I can add ones of spesific interest.
The complete file structure looks like this:
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
  Problem 1 (3.2G)
    A
    AA <-- an individual stock/etf
    ..
  Problem 2 (3.4G)
    A
    AA <-- an individual stock/etf
    ..
  Problem 3 (164G)
    A
    AA
      data.pkl <-- serialized trained model RandomForestRegressor
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
`http://projecticeman.pythonanywhere.com/predict?Symbol=IBM&vol_moving_avg=6.216773E6&adj_close_rolling_med=6.979269`  
  
![image](https://github.com/Project-Ice-Man/RiskThinking.AI/assets/124945757/126527e2-318f-4f65-ba69-ce8c6de1a654)
  
- Flask, which is a web application framework, was used to host the API
- `/predict` accepted 3 parameters: __Symbol__, __vol_moving_avg__, __adj_close_rolling_med__
- Folder /Problem 3/ uncompressed is 164G. Where I host the API it has disk space limit of 512 MB. Therefore I only serve 4: Apple (__AAPL__), IBM (__IBM__) Costco (__COST__) and Tesla (__TSLA__). I can manually add some more spesific ones that may be of interest, if disk space permits.
# Limitations and Approach
Due to limitations of personal computer, it did not seem possible to train `RandomForestRegressor()` on the entire dataset of more than 28 million rows. For example, training the model on only 30% of rows took about 22 minutes. Trying to serialize it caused a crush and left an incomplete serialized file of about 60 GB. In short: too small of a percentage of rows to train on; and too big of a resulting file when serialized.  
  
Therefore I took the approach of training the model per Symbol and serializing many individual models in /Problem 3/.  
  
Folder /Problem 3/ is 164G and takes 1 hour to zip, not enough room on Google Drive or API host to put on. Therefore, on the API host (512 MB limit) I only put 4: Costco, IBM, Tesla and Apple. On Google Drive I put the 4, plus a few more. I can add more to either, if some are of particular interest.
