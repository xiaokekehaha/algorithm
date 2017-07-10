package com.safe2345.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by zhangrb on 2017/6/4.
  */
trait Session {

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("spark")
//    .set("io.compression.codecs",
//      "org.apache.hadoop.io.compress.GzipCodec," +
//        "org.apache.hadoop.io.compress.DefaultCodec," +
//        "org.apache.hadoop.io.compress.SnappyCodec")
//    .set("mapreduce.map.output.compress","true")
//    .set("mapreduce.map.output.compress.codec",
//      "org.apache.hadoop.io.compress.SnappyCodec")
//    .set("mapreduce.admin.user.env",
//      "LD_LIBRARY_PATH=/usr/hdp/2.2.0.0-1084/hadoop/lib/native")
//    .set("yarn.app.mapreduce.am.env",
//      "LD_LIBRARY_PATH=$HADOOP_HOME/lib/native")
  val sc = new SparkContext(conf)

  val sparkSession = SparkSession
    .builder()
//    .enableHiveSupport()
    .master("local[*]")
    .appName("Spark Session")
    .getOrCreate()

}
