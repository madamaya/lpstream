#!/bin/zsh

mkdir -p ./results/metrics1/latency
mkdir -p ./results/metrics1/throughput
mkdir -p ./results/metrics2/latency
mkdir -p ./results/metrics2/throughput
mkdir -p ./results/metrics34

cp -r ./latency/metrics1/results ./results/metrics1/latency
cp -r ./throughput/metrics1/results ./results/metrics1/throughput
cp -r ./latency/metrics2/results ./results/metrics2/latency
cp -r ./throughput/metrics2/results ./results/metrics2/throughput
cp ./metrics34/*txt ./results/metrics34
