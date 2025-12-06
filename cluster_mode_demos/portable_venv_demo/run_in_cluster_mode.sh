#!/usr/bin/env bash
BASEDIR=$(dirname "$0")

source $BASEDIR/../../.venv/bin/activate

export HADOOP_CONF_DIR=/home/felix/Projects/spark-cluster-mode-demos/cluster_mode_demos/portable_venv_demo/hadoop_configs/quickstart-bigdata

spark-submit \
    --conf spark.eventLog.enabled=true  --master yarn  --deploy-mode cluster \
    cluster_entrypoint.py


    #    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
#    --archives venv_py37.tar.gz#environment \
