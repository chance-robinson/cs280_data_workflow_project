#!/bin/bash
export AIRFLOW_HOME=/home/chance/airflow-cs280
cd /home/chance/airflow-cs280
source /home/chance/miniconda3/bin/activate
conda activate /home/chance/miniconda3/envs/airflow-env
nohup airflow scheduler >> scheduler.log &
nohup airflow webserver -p 8080 >> webserver.log &