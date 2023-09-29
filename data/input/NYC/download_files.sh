#!/bin/bash

# yellow
<<C
for year in `seq 2018 2021`; do
  for month in `seq -w 1 12`; do
    if [ ${year} -ge 2023 ] && ( [ ${year} -gt 2023 ] || [ ${month} -gt 6 ] ); then
      continue
    fi
    #echo https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${year}-${month}.parquet;
    wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${year}-${month}.parquet -P ./parquet/yellow;
  done;
done
C

python parse_parquet-y.py

python sortcsv-y.py



<<COMMENT_OUT
# green
for year in `seq 2009 2023`; do
  for month in `seq 1 12`; do
    if [ ${year} -ge 2023 ] && ( [ ${year} -gt 2023 ] || [ ${month} -gt 6 ] ); then
      continue
    elif [ ${year} -le 2013 ] && ( [ ${year} -lt 2013 ] || [ ${month} -lt 7 ] ); then
      continue
    fi
    #echo https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_${year}-${month}.parquet
  done;
done

#FMV
for year in `seq 2009 2023`; do
  for month in `seq 1 12`; do
    if [ ${year} -ge 2023 ] && ( [ ${year} -gt 2023 ] || [ ${month} -gt 6 ] ); then
      continue
    elif [ ${year} -le 2014 ]; then
      continue
    fi
    echo https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_${year}-${month}.parquet
  done;
done

#FMVHV
for year in `seq 2009 2023`; do
  for month in `seq 1 12`; do
    if [ ${year} -ge 2023 ] && ( [ ${year} -gt 2023 ] || [ ${month} -gt 6 ] ); then
      continue
    elif [ ${year} -le 2019 ] && ( [ ${year} -lt 2019 ] || [ ${month} -lt 2 ] ); then
      continue
    fi
    echo https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_${year}-${month}.parquet
  done;
done
COMMENT_OUT