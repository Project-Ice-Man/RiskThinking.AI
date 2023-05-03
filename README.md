# RiskThinking.AI
I'm more familiar with solving problems 1-2 than 3-4. Therefore in Solution.py was able to implement 1 and 2 well (hopefully). For 3, I only plugged in the (nice) code that was provided and saved to disk as much details as I thought I should. I did not implement 4 at all, sorry about that.
# Solution.py
How it works will be described right after
```python
import os, shutil, time
import pandas as pd, numpy
...
print('\n--- %s seconds ---' % (time.time() - start_time))
```
# How it works
I could have spawned 10 processes via a shell script, but to see a little bit better what was going on I just opened 10 shell tabs and submitted 10 spark applications like this:
```
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py AB
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py CD
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py EF
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py GI <-- 29 mins
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py JL <-- 17 mins
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py MN
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py OQ
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py RS
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py TU
$: bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py VZ
```
It took 29 minutes to process stocks/etfs starting with letters G and I (GI). JL took the least amount of time - 17 minutes. And everything else was in between. Since all ran in parallel - 29 minutes was all it took to process them all.  
  
The processor had 12 cores - I kept starting a new `bin/spark-submit` in a new tab untill CPU was at about 100% with no processing power left. Therefore 10 tabs.
# Code Outline
I read `symbols_valid_meta.csv` line by line in a loop and for each stock/etf pefrormed the 3 required steps
# Apache Spark
I used Apache Spark to process data. It required installing Java. The rest can be installed via `pip install`
# Output
All output files are stored here - https://drive.google.com/drive/folders/1iTgZZ5kXTOiIKTtWwk39IOYFm3-qYszD?usp=sharing
```
RiskThinking.AI
  Errors
    AB <-- errors of bin/spark-submit /Volumes/Mac/Code/RiskThinking.AI/Solution.py AB
    CD
    ..
  Problem 1
    A
    AA <-- an individual stock/etf
    ..
  Problem 2
  Problem 3
```
