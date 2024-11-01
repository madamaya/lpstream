#!/bin/zsh

source ../config.sh

queries=(Syn1 Syn2 Syn3 LR Nexmark NYC YSB Nexmark2 NYC2 YSB2)
sizes=(-1 10 100 400)

# Real dataset
for idx in `seq 1 7`
do
  echo "./test2_main.sh ${idx} -1 |& tee test2_${idx}_-1.log"
  ./test2_main.sh ${idx} -1 |& tee test2_${idx}_-1.log
done

# Synthetic dataset
for size in 10 100 400
do
  for idx in `seq 8 10`
  do
    echo "./test2_main.sh ${idx} ${size} |& tee test2_${idx}_${size}.log"
    ./test2_main.sh ${idx} ${size} |& tee test2_${idx}_${size}.log
  done
done

cd ${L3_HOME}/bin/test/scripts
echo "(python test2.py "${queries}" "${sizes}" ${L3_HOME}/data/log)"
python test2.py "${queries}" "${sizes}" ${L3_HOME}/data/log
