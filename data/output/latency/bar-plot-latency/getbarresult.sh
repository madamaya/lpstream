#!/bin/zsh

L3_HOME=""

queries=(LR Nexmark Nexmark2 NYC NYC2 YSB YSB2 Syn1 Syn2 Syn3)
approaches=(baseline genealog l3stream l3streamlin)
sizes=(-1 10 100 400)

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
      echo "(python calcLatencyV2-bar.py 10 ${L3_HOME}/data/output/latEval/latency-bar/${query}/${approach} ${size} latency)"
      python calcLatencyV2-bar.py 10 ${L3_HOME}/data/output/latEval/latency-bar/${query}/${approach} ${size} latency
    done
  done
done

echo "(python resultsGen-bar.py "${queries}" "${approaches}" "${sizes}")"
python resultsGen-bar.py "${queries}" "${approaches}" "${sizes}"

# python resultsGen-bar2.py "LR Nexmark Nexmark2 NYC NYC2 YSB YSB2 Syn1 Syn2 Syn3" "baseline genealog l3stream l3streamlin" "-1 10 100 400"