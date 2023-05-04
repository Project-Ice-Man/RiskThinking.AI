# Intro
Hi there, I'm more familiar with solving problems like 1-2 than 3-4. Therefore in `Solution.py` I was able to implement 1 and 2 well (hopefully). For 3, I only plugged in the (nice) code that was provided and saved to disk as much details as I thought was required. I did not implement 4 at all, sorry about that.
# Solution.py
How it works will be described right after
```python
import os, shutil, time
import pandas as pd, numpy

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

sc = SparkContext('local'); sc.setLogLevel('ERROR')
spark = SparkSession(sc);   spark.conf.set('mapreduce.fileoutputcommitter.marksuccessfuljobs', 'false') # to not write _SUCCESS

# read
input_path = '/Volumes/Mac/Code/RiskThinking.AI'

# write
output_path = '/Volumes/Windows/Win/Code/RiskThinking.AI'

# if any
errors = []

# etfs or stocks
schema = StructType([ StructField('Date',     StringType(), True),
                      StructField('Open',      FloatType(), True),
                      StructField('High',      FloatType(), True),
                      StructField('Low',       FloatType(), True),
                      StructField('Close',     FloatType(), True),
                      StructField('Adj Close', FloatType(), True),
                      StructField('Volume',    FloatType(), True) ])

start_time = time.time()

# to process a range of csv files starting with these letters, for example:
# $: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py AM
min_letter = sys.argv[1][0] # 'A' for example
max_letter = sys.argv[1][1] # 'M' for example

# a few columns from symbols_valid_meta
symbols_valid_meta = ( spark.read.csv(f'{input_path}/stock-market-dataset/archive/symbols_valid_meta.csv', header=True)
                            .select('Symbol', 'Security Name', 'ETF')
                            .where(f'upper(substring(Symbol, 1, 1)) between "{min_letter}" and "{max_letter}"')
                            .orderBy('Symbol'))
# 30 days
w = Window.partitionBy('Symbol').orderBy('Date').rowsBetween(-29, Window.currentRow)

count = 1
count_max = symbols_valid_meta.count()

for svm in symbols_valid_meta.collect():
    Symbol       = svm['Symbol']
    SecurityName = svm['Security Name']
    folder = 'etfs' if svm['ETF'] == 'Y' else 'stocks'

    pct = '{:.3%}'.format(count/count_max) # percentage of processed files
    prg = ' '.join([ pct.rjust(8,' '),
                  folder.ljust(6,' '),
                  Symbol.ljust(5,' '),
                  SecurityName     ]); print('\r', (prg[:98] + '..') if len(prg) > 100 else prg, end='') # print progress
    try:
        # Problem 1 ------------------------------------------------------------
        df = ( spark
            .read.format('csv').load(f'{input_path}/stock-market-dataset/archive/{folder}/{Symbol}.csv', header=True, schema=schema)

            .withColumn('Symbol',        lit(Symbol))
            .withColumn('Security Name', lit(SecurityName))

            .select('Symbol', 'Security Name', 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume')) # columns in the right order

        df.write.format('parquet').mode('overwrite').save(f'{output_path}/Problem 1/{Symbol}') # save()

        # Problem 2 ------------------------------------------------------------
        df = ( df
            .withColumn('vol_moving_avg',        avg('Volume')   .over(w))
            .withColumn('adj_close_rolling_med', avg('Adj Close').over(w))) # two more columns

        df.write.format('parquet').mode('overwrite').save(f'{output_path}/Problem 2/{Symbol}') # save()

        # Problem 3 ------------------------------------------------------------
        data = df.toPandas()

        # Assume `data` is loaded as a Pandas DataFrame
        data['Date'] = pd.to_datetime(data['Date'])
        data.set_index('Date', inplace=True)

        # Remove rows with NaN values
        data.dropna(inplace=True)

        # Select features and target
        features = ['vol_moving_avg', 'adj_close_rolling_med']
        target = 'Volume'

        X = data[features]
        y = data[target]

        # Split data into train and test sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Create a RandomForestRegressor model
        model = RandomForestRegressor(n_estimators=100, random_state=42)

        # Train the model
        model.fit(X_train, y_train)

        # Make predictions on test data
        y_pred = model.predict(X_test)

        # Calculate the Mean Absolute Error and Mean Squared Error
        mae = mean_absolute_error(y_test, y_pred)
        mse = mean_squared_error(y_test, y_pred)

        if not os.path.exists(f'{output_path}/Problem 3/{Symbol}'):
            os.makedirs(f'{output_path}/Problem 3/{Symbol}')

        data    .to_pickle(f'{output_path}/Problem 3/{Symbol}/data.pkl')
        X_train .to_pickle(f'{output_path}/Problem 3/{Symbol}/X_train.pkl')
        X_test  .to_pickle(f'{output_path}/Problem 3/{Symbol}/X_test.pkl')
        y_train .to_pickle(f'{output_path}/Problem 3/{Symbol}/y_train.pkl')
        y_test  .to_pickle(f'{output_path}/Problem 3/{Symbol}/y_test.pkl')
        numpy   .save     (f'{output_path}/Problem 3/{Symbol}/y_pred.npy', y_pred) # save()

    except Exception as x:
        errors += [[Symbol, f'{x}']]

    count += 1
    # if count > 10: break

if len(errors):
    print('\n')
    schema = StructType([ StructField('Symbol', StringType(), True),
                          StructField('Error',  StringType(), True)])

    df = spark.createDataFrame(errors, schema)
    df.show(truncate=False)
    df.write.format('parquet').mode('overwrite').save(f'{output_path}/Errors/{sys.argv[1]}') # save()

    print(f'The total number of errors for {sys.argv[1]}: {df.count()}')

print('\n\n--- %s seconds ---' % (time.time() - start_time))
```
# How it works
A bash script starts 10 processes in parallel (to fill up CPU's capacity to about 100%):  

`sergei_kachanov spark-3.4.0-bin-hadoop3 $: bash /Volumes/Mac/Code/RiskThinking.AI/Run.sh`  
  
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
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py AB <-- 29 mins
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py CD
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py EF
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py GI
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py JL
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py MN
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py OQ
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py RS
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py TU
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py VZ <-- 18 mins
```
It took 29 minutes to process stocks/etfs starting with letters A and B (AB). 'VZ' took the least amount of time - 18 minutes. And everything else was in between. Since all ran in parallel - 29 minutes was all it took to process them all.  
  
From previous tests I figured how to group them into 10 groups more or less evenly.  
  
Reading (all the original stocks/etfs) was done from one SSD, writing was done to another SSD.
# Code Outline
I read `symbols_valid_meta.csv` line by line in a loop and for each stock/etf pefrormed the 3 required steps
# Apache Spark
I used Apache Spark to process data. It required installing Java. The rest could be easily installed via `pip install`
# Output
All output files are stored here - https://drive.google.com/drive/folders/1iTgZZ5kXTOiIKTtWwk39IOYFm3-qYszD?usp=sharing
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
      X_test.pkl <-- what I whought I needed to save (I may have gotten it wrong, sorry if I did)
      X_train.pkl
      data.pkl
      y_pred.npy
      y_test.pkl
      y_train.pkl
    ..
```
# Errors
To see the types of errors encountered in the process:
```python
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
