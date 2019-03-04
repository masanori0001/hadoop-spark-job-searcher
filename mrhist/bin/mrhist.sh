#!/bin/bash

BIN_DIR=$(cd "$(dirname "$0")";pwd)
JAR=$(cd "$(dirname "$0")";pwd)/../dist/histlogmon-jar-with-dependencies.jar

java -cp ${JAR} com.dena.hadoopLogMon.mrhist.MrHist "$@"
