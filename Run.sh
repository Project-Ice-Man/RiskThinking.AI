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
