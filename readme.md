
# IMPORT KEYSPACE/TABLES FROM CASSANDRA TO HIVE USING PYSPARK

This project is to import the keyspace with table from cassandra using pyspark. Code is written in pyspark with Cassandra Spark Connector Jar.

#### **Components:**

* Config File
* Environment file
* Pyspark Code
* Wrapper Script


##### ConfigFile
Config file will have details regarding database/host and port etc
```bash
[<keyspace_name>]
hostname=10.42.18.5
port=9042
table_list=tab1,tab2
partition=None
```


##### Environment file
This will have secret configs and exported on runtime only.
```bash
export user=admin
export password=admin
export target_path=/tmp/tables
# format orc, parquet
export format=parquet
# mode can be ignore, overwrite, append
export mode=overwrite
```

##### Wrapper Script 
we need to execute this script only
```bash
source ./env_files/env.sh
# spark-submit --jars spark-cassandra-connector.jar cassandraToHive.py <config> <keyspace>
spark-submit --jars spark-cassandra-connector.jar cassandraToHive.py configs/config.ini test
```

###Steps:

You need to clone the repo