import findspark
import argparse
import os
from pyspark.sql import SparkSession

findspark.init()

def get_args():
    parser = argparse.ArgumentParser(description='Spark Job on xkcd data stored within HDFS.')
    parser.add_argument('--year', help='Partion Year To Process, e.g. 2019', required=True, type=str)
    parser.add_argument('--month', help='Partion Month To Process, e.g. 10', required=True, type=str)
    parser.add_argument('--day', help='Partion Day To Process, e.g. 31', required=True, type=str)
    parser.add_argument('--hdfs_source_dir', help='HDFS source directory, e.g. /user/hadoop/xkcd/raw', required=True, type=str)
    parser.add_argument('--hdfs_target_dir', help='HDFS target directory, e.g. /user/hadoop/xkcd/final', required=True, type=str)

    return parser.parse_args()

if __name__ == '__main__':
    args = get_args()

    spark = SparkSession.builder.master("yarn").appName("xkcd").getOrCreate()

    #Read the master json 
    df_xkcd = spark.read.option("multilin:ee","true").json("/user/hadoop/xkcd/raw/" + args.year + "_" + args.month + "_" + args.day +"/")
    
    #Select relevant columns --> "drop" the other columns
    df_xkcd = df_xkcd.select('num', 'year', 'title', 'img')

    #Drop Rows with null values
    df_xkcd = df_xkcd.na.drop()

    #Repartition data by year and save as parquet and external table
    df_xkcd.repartition('year').write.format("parquet").mode("overwrite").option('path', args.hdfs_target_dir).partitionBy('year').saveAsTable('default.xkcd_partitioned')

    #Write data to MySQL Database --> "mode:update" to update data and not e.g. append 
    df_xkcd.write.format("jdbc").option("url","jdbc:mysql://mysql-server/xkcd_database?characterEncoding=UTF-8").option("dbtable","xkcd_datatable").option("user","root").option("password","bootroot42").option("driver", "com.mysql.jdbc.Driver").mode("overwrite").save()