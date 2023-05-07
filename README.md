# Intro
Hi there, there are 2 .py files in this solution:
1. `Solution.py` - provides solution from Problems 1-3
2. `Predict.py` - an API service to serve the trained predictive model (Problem 4)
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
# from sklearn.ensemble import RandomForestRegressor
# from sklearn.metrics import mean_absolute_error, mean_squared_error

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
        # model = RandomForestRegressor(n_estimators=100, random_state=42)

        # Train the model
        # model.fit(X_train, y_train)

        # Make predictions on test data
        # y_pred = model.predict(X_test)

        # Calculate the Mean Absolute Error and Mean Squared Error
        # mae = mean_absolute_error(y_test, y_pred)
        # mse = mean_squared_error(y_test, y_pred)

        if not os.path.exists(f'{output_path}/Problem 3/{Symbol}'):
            os.makedirs(f'{output_path}/Problem 3/{Symbol}')

        # data    .to_pickle(f'{output_path}/Problem 3/{Symbol}/data.pkl')
        X_train .to_pickle(f'{output_path}/Problem 3/{Symbol}/X_train.pkl')
        X_test  .to_pickle(f'{output_path}/Problem 3/{Symbol}/X_test.pkl')
        y_train .to_pickle(f'{output_path}/Problem 3/{Symbol}/y_train.pkl')
        y_test  .to_pickle(f'{output_path}/Problem 3/{Symbol}/y_test.pkl')
        # numpy   .save     (f'{output_path}/Problem 3/{Symbol}/y_pred.npy', y_pred) # save()

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

print(f'\n\n--- Run Time: {int(time.time() - start_time)} seconds ---')
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
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py AB <-- 15 mins
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py CD
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py EF
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py GI
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py JL
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py MN
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py OQ
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py RS
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py TU
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py VZ <-- 10 mins
```
It took 15 minutes to process stocks/etfs starting with letters A and B (AB). 'VZ' took the least amount of time - 10 minutes. And everything else was in between. Since all ran in parallel - 15 minutes was all it took to process them all.  
  
From previous tests I figured how to group them into 10 groups more or less evenly.  
  
Reading (all the original stocks/etfs) was done from one SSD, writing was done to another SSD.
# Code Outline
I read `symbols_valid_meta.csv` line by line in a loop and for each stock/etf pefrormed the 3 required steps
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
      X_test.pkl
      X_train.pkl
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
# API service - Predict.py
```python
from flask import Flask, request

import pandas as pd, time
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

app = Flask(__name__)

@app.route('/predict')
def predict():
    try:
        start_time = time.time()

        # query string | parameters
        args = request.args

        # query string | values
        Symbol                = args.get('Symbol')
        vol_moving_avg        = args.get('vol_moving_avg')
        adj_close_rolling_med = args.get('adj_close_rolling_med')

        # train files saved during pipeline execution in folder /Problem 3/
        X_train = pd.read_pickle(f'/Volumes/Windows/Win/Code/RiskThinking.AI/Problem 3/{Symbol}/X_train.pkl')
        y_train = pd.read_pickle(f'/Volumes/Windows/Win/Code/RiskThinking.AI/Problem 3/{Symbol}/y_train.pkl')

        # Create a RandomForestRegressor model
        model = RandomForestRegressor(n_estimators=100, random_state=42)

        # Train the model
        model.fit(X_train, y_train)

        # test files saved during pipeline execution in folder /Problem 3/
        X_test = pd.read_pickle(f'/Volumes/Windows/Win/Code/RiskThinking.AI/Problem 3/{Symbol}/X_test.pkl')
        y_test = pd.read_pickle(f'/Volumes/Windows/Win/Code/RiskThinking.AI/Problem 3/{Symbol}/y_test.pkl')

        # Make predictions on test data
        y_pred = model.predict(X_test)

        # Calculate the Mean Absolute Error and Mean Squared Error
        mae = mean_absolute_error(y_test, y_pred)
        mse = mean_squared_error(y_test, y_pred)

        # predicting
        pred = ( model.predict(
            pd.DataFrame({'vol_moving_avg': [vol_moving_avg],
                   'adj_close_rolling_med': [adj_close_rolling_med]}) ))
        # html
        return ('<table>\n<tr>' +

            '\n<tr>'.join([ f'<td class=right>                Symbol  <td> {Symbol}',
                            f'<td class=right>        vol_moving_avg  <td> {float(vol_moving_avg):,.2f}',
                            f'<td class=right> adj_close_rolling_med  <td> {float(adj_close_rolling_med):,.6f}',
                            f'<td class=right>      Predicted Volume  <th> {int(pred[0]):,}',
                            f'<td class=right>   Mean Absolute Error  <td> {mae:,.2f}',
                            f'<td class=right>   Mean  Squared Error  <td> {mse:,.2f}',
                            f'<td class=right>        Execution Time  <td> {round(time.time() - start_time, 2):.2f} sec' ]) +

            '\n</table>\n<style> td, th {font: 9pt Verdana} th {text-align: left; background-color: lemonchiffon} .right {text-align: right; padding-right: 10} </style>')

    # in case something bad happened
    except Exception as x:
        return f'<pre>{x}</pre>'

if __name__ == "__main__":
    app.run()
```
And a screenshot
![image](https://user-images.githubusercontent.com/124945757/236707366-aa2130c4-6f85-43cb-b2f8-7e7330732c67.png)
