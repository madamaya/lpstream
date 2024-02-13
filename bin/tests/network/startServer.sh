#!/bin/zsh

source $(dirname $0)/../../config.sh

java -cp ${JAR_PATH} com.madamaya.l3stream.tests.network.NetworkOverheadServer localhost 19999 1000