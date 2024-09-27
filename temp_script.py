#!/usr/bin/python
# -*- coding: utf-8 -*-

# -------------------------------------------------------------------------------------
# Generic Pipeline which can execute mutiple data loads
# Parameters :
#         --TASK_TYPE            Task Group
#         --TASK_NAME            Individual Task Name
#         --PROCESS_INSTANCE_ID  Session ID
# -------------------------------------------------------------------------------------
import os #version2
import pyspark
import argparse
#from delta import *
from pyspark.sql import SparkSession
from ocidl.util.secret import GetSecretText #custome module
from ocidl.workflow import deltaflow #custome module
from pyspark.sql.types import StringType, StructField, StructType,TimestampType, IntegerType
from pyspark.sql.functions import regexp_extract, expr, when, col, lit,replace, current_timestamp,udf
#import multiprocessing as mp
#from multiprocessing.pool import ThreadPool
spark = SparkSession.builder.appName('Generic Query Executer').config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED").getOrCreate()
#asyncResults = []
#Get the Datalake Configurations 
df_config = spark.read.json('config.json').cache()
#Global Variables
config_db = df_config.select('config_db').collect()[0][0]
NumOfPartitions = df_config.select('NumOfPartitions').collect()[0][0]
DataBase_OCID = df_config.select('db_ocid').collect()[0][0]
namespace = df_config.select('namespace').collect()[0][0]
metastore_bucket = df_config.select('metastore_bucket').collect()[0][0]
landing_bucket = df_config.select('landing_bucket').collect()[0][0]
DB_User_Name = df_config.select('db_user').collect()[0][0]
DB_Vault_Ocid = df_config.select('db_vault_ocid').collect()[0][0]
print("Call vault")
DB_Password = GetSecretText(spark,DB_Vault_Ocid) #Get password from the vault
df_config = df_config.withColumn('db_password',lit(DB_Password))



# -------------------------------------------------------------------------------------
# get the active tasks to be executed in a loop
# it can get the configuration data from oracle or delta tables
# this method will be called once at the begening of the application
# -------------------------------------------------------------------------------------

def get_workflow_config(
    properties,
    ):

    if config_db == 'oracle':
        etl_workflow_query = "select * from DL_ETL_WORKFLOW_CONFIGURATION where IS_ACTIVE='Y' and EXECUTION_BY='OCI_DATAFLOW' ORDER BY WORKFLOW_ORDER"
        df_workflow_config = spark.createDataFrame([], StructType([]))
        df_workflow_config = spark.read.format('oracle').option('query',etl_workflow_query).options(**properties).load()
        df_workflow_config = df_workflow_config.withColumn('DL_WORKFLOW_ID',col('DL_WORKFLOW_ID').cast(IntegerType()))
        for c in df_workflow_config.columns:
            df_workflow_config = df_workflow_config.withColumnRenamed(c,c.lower())
        df_workflow_config.printSchema()
    elif config_db == 'delta':
        if task_name == 'ALL':
            etl_config_query = "select * from suk_datalake.dl_etl_task_configuration where is_active='Y' and task_type= '" \
                + task_type + "' order by order_of_execution"
        else:
            etl_config_query = "select * from suk_datalake.dl_etl_task_configuration where is_active='Y' and task_type= '" \
                + task_type + "' and task_name='" + task_name \
                + "' order by order_of_execution"
        df_etl_config = spark.sql(etl_config_query)
        df_etl_config.printSchema()
    return df_workflow_config
def check_workflow_status(workflow_name,dataflow_run_id,process_instance_id,properties):

    if config_db == 'oracle':
        workflow_count_qry = "select count(*) cnt from dl_etl_task_execution_details where task_type='"+workflow_name+"' and process_instance_id='"+process_instance_id+"' and dataflow_run_id ='"+dataflow_run_id+"' and status <> 'Success'"
        df_workflow_count = spark.createDataFrame([], StructType([]))
        df_workflow_count = spark.read.format('oracle').option('query',workflow_count_qry).options(**properties).load()
        workflow_count = df_workflow_count.select('cnt').collect()[0][0]
        if workflow_count > 0:
           return 'Error'
        else:
           return 'Success'
    elif config_db == 'delta':
        if task_name == 'ALL':
            etl_config_query = "select * from suk_datalake.dl_etl_task_configuration where is_active='Y' and task_type= '" \
                + task_type + "' order by order_of_execution"
        else:
            etl_config_query = "select * from suk_datalake.dl_etl_task_configuration where is_active='Y' and task_type= '" \
                + task_type + "' and task_name='" + task_name \
                + "' order by order_of_execution"
        df_etl_config = spark.sql(etl_config_query)
        df_etl_config.printSchema()
    return 'NA'
def main():

# ----------------------------------------#
    print('Set the variables')
# ----------------------------------------#
    print('Parse command line arguments')
    parser = argparse.ArgumentParser(description='Pass the arguments.')
    #parser.add_argument('--TASK_TYPE')
    #parser.add_argument('--TASK_NAME')
    parser.add_argument('--PROCESS_INSTANCE_ID')
    args = parser.parse_args()
    properties = {
        'adbId': DataBase_OCID,
        'user': DB_User_Name,
        'password': DB_Password,
        'numPartitions': NumOfPartitions,
        }

    # Get Arguments into variables

    #task_name = args.TASK_NAME
    #task_type = args.TASK_TYPE
    process_instance_id = args.PROCESS_INSTANCE_ID
    #data_source_num_id = '112'
    dataflow_run_id = os.getenv('DATAFLOW_RUN_ID')
    df_workflow_config = get_workflow_config(properties)  # Get configuration details
    data_collect = df_workflow_config.collect()
    workflow = deltaflow(df_config,spark)
    print("created class instance")
    print(workflow.namespace)
    print(workflow.DataBase_OCID)
    for row in data_collect:
     print ('Startin the workflow : '+row.workflow_name)
     workflow.execute(row.workflow_name,'ALL',process_instance_id)
     if check_workflow_status(row.workflow_name,dataflow_run_id,process_instance_id,properties) == 'Error':
        raise Exception("Some of the tasks in the workflow "+row.workflow_name+" failed , Please Correct and re-run")
     print ('Workflow Completed: '+row.workflow_name)     
    print ('All the Workflows completed')
if __name__ == '__main__':
    main()
