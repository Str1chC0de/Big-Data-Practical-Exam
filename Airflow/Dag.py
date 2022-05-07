import os
from os import listdir
from os.path import isfile, join
import glob
import json
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.bash_operator import BashOperator
from hooks.hdfs_hook import HdfsHook


default_args = {
    'owner' : 'Simo'
}

#Define Dag
dag = DAG (
    dag_id = 'XKCD_dag',
    default_args = default_args,
    description = 'XKCD - Practical exam',
    schedule_interval = timedelta(minutes = 1440),
    start_date = datetime(2021, 11, 15),
    catchup = False,
    max_active_runs = 1
)

create_local_import_dir = CreateDirectoryOperator(
    task_id = 'create_import_dir',
    path = '/home/airflow/airflow',
    directory = 'xkcd',
    dag = dag,
)

clear_local_import_dir = ClearDirectoryOperator(
    task_id = 'clear_import_dir',
    directory = '/home/airflow/airflow/xkcd',
    pattern = '*',
    dag = dag,
)

#Bash Operator to call the data_collector.py
get_data = BashOperator (
    task_id = 'get_data',
    bash_command = 'python ~/airflow/python/data_collector.py',
    dag = dag
)

create_hdfs_xkcd_raw_dir = HdfsMkdirFileOperator(
    task_id = 'create_hdfs_xkcd_raw_dir',
    directory = '/user/hadoop/xkcd/raw/',
    hdfs_conn_id = 'hdfs',
    dag = dag,
)

create_hdfs_xkcd_final_dir = HdfsMkdirFileOperator(
    task_id = 'create_hdfs_xkcd_final_dir',
    directory = '/user/hadoop/xkcd/final/',
    hdfs_conn_id = 'hdfs',
    dag = dag,
)

#Transfer the single jsons to one master json
def convert_Data():
    #define paths
    directory_airflow = "/home/airflow/airflow/xkcd/"
    master_file = open("/home/airflow/airflow/xkcd/master.json", "w")

    #create list of filenames in the target directory 
    files = [f for f in listdir(directory_airflow) if isfile(join(directory_airflow, f))]
    dfs = []

    #for each file, read and than append to a list
    for x in files:
        data = pd.read_json(directory_airflow + "/" + x, lines=True)
        dfs.append(data)

    #Convert the list to dataframe with pandas
    df = pd.concat(dfs, ignore_index=True)
    #write the dataframe to master.json file
    df.to_json(master_file, orient="records")
    master_file.close()


create_master_json = PythonOperator(
    task_id='create_master_json',
    python_callable= convert_Data,
    dag=dag,
)

hdfs_put_master_json = HdfsPutFileOperator(
    task_id='hdfs_put_master_json',
    local_file='/home/airflow/airflow/xkcd/master.json',
    remote_file='/user/hadoop/xkcd/raw/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}_{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}_{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/master.json',
    hdfs_conn_id='hdfs',
    dag=dag,
)

partitionate_hdfs_final = SparkSubmitOperator(
    task_id = 'xkcd_partitioned_by_year',
    conn_id = 'spark',
    application = '/home/airflow/airflow/python/hdfs_partitioner.py',
    total_executor_cores = '2',
    executor_cores = '2',
    executor_memory = '2g',
    num_executors = '2',
    name = 'xkcd_partitioned_by_year',
    verbose = True,
    application_args = ['--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}', '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}', '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}', '--hdfs_source_dir', '/user/hadoop/xkcd/raw/', '--hdfs_target_dir', '/user/hadoop/xkcd/final'],
    dag = dag
)
#Main branch
create_local_import_dir >> clear_local_import_dir >> get_data
get_data >> create_master_json >> hdfs_put_master_json >> partitionate_hdfs_final

#Secendary branch
clear_local_import_dir >> create_hdfs_xkcd_raw_dir >> create_hdfs_xkcd_final_dir
