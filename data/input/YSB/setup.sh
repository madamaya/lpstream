#!/bin/bash

git clone https://github.com/yahoo/streaming-benchmarks.git
mkdir streaming-benchmarks/data/util
cp *py streaming-benchmarks/data/util
cp project.clj streaming-benchmarks/data
cd streaming-benchmarks/data
lein run -n --configPath ../conf/benchmarkConf.yaml
lein run -r -t 10000 --configPath ../conf/benchmarkConf.yaml