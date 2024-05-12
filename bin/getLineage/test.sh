#!/bin/zsh

source ../config.sh

queries=(1 2 3 4 5 6 7 8 9 10)
sizes=(-1 10 100 400)
pivot=8
for size in ${sizes[@]}
do
  for query in ${queries[@]}
  do
    # Skip invalid cases
    if [[ ${query} -ge ${pivot} ]]; then
      if [ ${size} -eq -1 ]; then
        continue
      fi
    else
      if [ ${size} -ne -1 ]; then
        continue
      fi
    fi

    echo "Next: query=${query}, size=${size}"
    echo "(./LMUserDriver.sh ${query} ${size} 10000 | tee progress.${query}.${size}.log)"
    #./LMUserDriver.sh ${query} ${size} 10000 | tee progress.${query}.${size}.log
  done
done
