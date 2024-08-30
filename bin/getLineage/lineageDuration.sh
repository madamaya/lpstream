#!/bin/zsh

source ../config.sh

# Real dataset
for idx in `seq 1 5`
do
  echo "./LMUserDriver.sh ${idx} -1 |& tee lineageDuration_${idx}_-1.log"
  ./LMUserDriver.sh ${idx} -1 |& tee lineageDuration_${idx}_-1.log
done

# Synthetic dataset
for size in 10 50 100
do
  for idx in `seq 8 10`
  do
    echo "./LMUserDriver.sh ${idx} ${size} |& tee lineageDuration_${idx}_${size}.log"
    ./LMUserDriver.sh ${idx} ${size} |& tee lineageDuration_${idx}_${size}.log
  done
done

cd ${L3_HOME}/data/output/lineage
echo "python calcDerivationTime.py"
python calcDerivationTime.py