#!/usr/bin/env elvish

# pants package airflow:pex
pants package airflow:pex

# PEX=`pwd`/dist/airflow/pex.pex
var PEX = (pwd)/dist/airflow/pex.pex

# AIRFLOW_SCRIPT=`pwd`/bin/airflow
var AIRFLOW_SCRIPT = (pwd)/bin/airflow

# TARGET_PYTHON=$HOME/airflow/bin/python
var TARGET_PYTHON = $E:HOME/airflow/bin/python

# TARGET_AIRFLOW=$HOME/bin/airflow
var TARGET_AIRFLOW = $E:HOME/bin/airflow

# cmp --silent $PEX $TARGET_PYTHON || \
#   (mkdir -p $HOME/airflow/bin && \
#    cp $PEX $TARGET_PYTHON && \
#    echo "Deloyed to $TARGET_PYTHON")
try { cmp --silent $PEX $TARGET_PYTHON } catch e {
    echo "The PEX for Python differs"
    mkdir -p $E:HOME/airflow/bin
    cp $PEX $TARGET_PYTHON
    echo "Deployed to "$TARGET_PYTHON
} else {
    echo "The PEX for Python is the same, no need to install"
}

# cmp --silent $AIRFLOW_SCRIPT $TARGET_AIRFLOW || \
#   (mkdir -p $HOME/bin && \
#    cp $AIRFLOW_SCRIPT $TARGET_AIRFLOW && \
#    echo "Deloyed to $TARGET_AIRFLOW ")
try { cmp --silent $AIRFLOW_SCRIPT $TARGET_AIRFLOW } catch e {
    echo "The script for Airflow differs"
    mkdir -p $E:HOME/airflow/bin
    cp $PEX $TARGET_AIRFLOW
    echo "Deployed to "$TARGET_AIRFLOW
} else {
    echo "The script for Airflow is the same, no need to install"
}

