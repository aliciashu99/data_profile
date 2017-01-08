#!/usr/bin/env bash

# Use this for running in the cluster mode.

/Users/ashu/spark-2.0.2-bin-hadoop2.7/bin/spark-submit \
  --class io.infoworks.TableProfiler \
  --num-executors 1 \
  --driver-memory 1g \
  --executor-memory 1g \
  --executor-cores 1 \
  --jars /Users/ashu/spark-2.0.2-bin-hadoop2.7/jars/datanucleus-api-jdo-3.2.6.jar,/Users/ashu/spark-2.0.2-bin-hadoop2.7/jars/datanucleus-rdbms-3.2.9.jar,/Users/ashu/spark-2.0.2-bin-hadoop2.7/jars/datanucleus-core-3.2.10.jar \
  /Users/ashu/dev/data_profile/target/data_profile-1.0-SNAPSHOT.jar alicia airline \
  $@
