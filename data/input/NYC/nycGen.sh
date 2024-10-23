#!/bin/bash

echo "=*=*=*=*= Start NYC data generation =*=*=*=*="

st=2017
ed=2023
if [ $# -eq 2 ]; then
  st=$1
  ed=$2
fi

# yellow
echo "*** Download raw files (${st} -- ${ed}) ***"
for year in `seq ${st} ${ed}`; do
  for month in `seq -w 1 12`; do
    if [ ${year} -ge 2023 ] && ( [ ${year} -gt 2023 ] || [ ${month} -gt 6 ] ); then
      continue
    fi
    if [ ${year} -le 2017 ] && ( [ ${year} -lt 2017 ] || [ ${month} -lt 11 ] ); then
      continue
    fi
    wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${year}-${month}.parquet -P ../data/parquet;
  done;
done

echo "*** Parse files ***"
python parse_parquet-y.py ${st} ${ed}

echo "*** Sort data by dropoff timestamp ***"
python sortcsv-y.py

echo "cp ../data/nyc.csv ../data/nyc2.csv"
cp ../data/nyc.csv ../data/nyc2.csv

echo "=*=*=*=*= End NYC data generation =*=*=*=*="