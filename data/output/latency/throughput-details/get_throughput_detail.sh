#!/bin/zsh

L3_HOME="/home/yamada/l3stream"
TH_DIRNAME="thEval20240712-phase2"

queries=(LR Nexmark Nexmark2 NYC NYC2 YSB YSB2 Syn1 Syn2 Syn3)
approaches=(baseline genealog l3stream l3streamlin)
sizes=(-1 10 100 400)

for idx in `seq 1 9`
do
  for query in ${queries[@]}
  do
    for approach in ${approaches[@]}
    do
      for size in ${sizes[@]}
      do
        if [[ ${query} == *Syn* ]]; then
          if [ ${size} -eq -1 ]; then
            continue
          fi
        else
          if [ ${size} -ne -1 ]; then
            continue
          fi
        fi
        echo "(python calcLatencyV2-throughput-detail.py 10 ${L3_HOME}/data/output/${TH_DIRNAME}/latency${idx}/${query}/${approach} ${size} latency)"
        python calcLatencyV2-throuthput-detail.py 10 ${L3_HOME}/data/output/${TH_DIRNAME}/latency${idx}/${query}/${approach} ${size} latency
      done
    done
  done

  echo "(python resultsGen-throughput-detail.py "${queries}" "${approaches}" "${sizes}" ${L3_HOME}/data/output/${TH_DIRNAME}/latency${idx})"
  python resultsGen-throughput-detail.py "${queries}" "${approaches}" "${sizes}" ${L3_HOME}/data/output/${TH_DIRNAME}/latency${idx}
done