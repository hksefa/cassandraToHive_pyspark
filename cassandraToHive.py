# **********************************************************************************************************************************
# Auther       : Vikas Srivastava
# Description  : This utility will connect to cassandra cluster using pyspark and extract the data table wise and write to your
#                destination path (hdfs/s3) in parquet/CSV format. In order to acheive the desired output you need to call
#                this pyspark script from shell script or any script specifying below variables.
# How to Call  : spark-submit --jars <spark-cassandra-connector.jar> <cassandra_load.py> <config.ini path> <keyspace name>
# Pre-Requiste : Below variables with the same name mentioned below to be exported prior to call the script.
#                database, user, password, query, target_path, op_format(Output format should be 'parquet' or
#                'csv'), op_mode(Output mode should be 'overwrite' or 'append') and delimiter(if op_format is csv)
# **********************************************************************************************************************************
from datetime import datetime
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
import configparser
import os
import sys


def main():
    config_file = sys.argv[1].lower()
    section = sys.argv[2].lower()
    if config_file is None:
        sys.exit(1)
    else:
        spark_load(config_file, section)


def log(type, msg):
    print("{}:{}: {}".format(str(datetime.now()), type.upper(), msg))


def spark_load(config_file, section):
    appName = "PySpark Cassandra query Load"
    master = "local"
    spark = SparkSession.builder.config("spark.sql.parquet.writeLegacyFormat", "true").appName(
        appName).enableHiveSupport().master(master).getOrCreate()
    config = configparser.ConfigParser()
    config.read(config_file)
    sections = config.sections()
    keyspace = section
    server = config[section]['hostname']
    port = config[section]['port']
    table_list = config[section]['table_list']
    partition = config[section]['partition']
    user = os.environ['user']
    password = os.environ['password']
    target_path = os.environ['target_path']
    op_format = os.environ['format']
    op_mode = os.environ['mode']
    log("info", "pyspark script to extract data from cassendra server is starting. Time")
    print("-----------------------------------------------------------------------------------------------------------------")
    print("Server Name      : " + server)
    print("Port Number      : " + port)
    print("User Name        : " + user)
    print("Keyspace Name    : " + keyspace)
    print("Destination path : " + target_path)
    print("Output Format    : " + op_format)
    print("Mode of Output   : " + op_mode)
    print("Table List       : " + table_list)
    print("-----------------------------------------------------------------------------------------------------------------")

    for table in table_list.split(','):
        log("info", "**** Running for Table {} ***".format(table))
        cassDF = spark.read.format("org.apache.spark.sql.cassandra").option("spark.cassandra.connection.host", server).option(
            "encoding", "ascii").options(table=table, keyspace=keyspace).load()
        cassDF.show()
        cassDF.printSchema()
        record_count = cassDF.count()
        log("info", "Query read completed and loaded into spark dataframe")
        log("info", "Starting load to datalake target path")
        log("info", "Record count is "+str(record_count))
        try:
            target_path = "{}/{}/{}".format(target_path, keyspace, table)
            partition = None if partition.lower() == "none" else partition
            log("info", "partition : {}".format(type(partition)))
            if op_format == "csv":
                log("warn", "you need to be create csv table for {} with path {}".format(
                    table, target_path))
            spark.sql("CREATE database IF NOT EXISTS {}".format(keyspace))
            cassDF.write.saveAsTable(keyspace+"."+table, format=op_format, mode=op_mode,  partitionBy=partition,
                                     path=target_path)
        except:
            log("error", "file format is not correct, please check !!")
            sys.exit(100)
        log("info", "dataframe loaded in {} format successfully into target path {}".format(
            op_format, target_path))
        log("info", "Data copyied for table {} successfully".format(table))


if __name__ == '__main__':
    main()
