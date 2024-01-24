#!/bin/bash

cd ../data
for idx in `seq 0 $(($1-1))`
do
  for flag in `seq 2 3`
  do
    cp syn1.$2.csv.ingest.${idx} syn${flag}.csv.ingest.${idx}
  done
done