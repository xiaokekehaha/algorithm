package com.safe2345.hive

import java.util.Properties

import com.safe2345.utils.Session
import jodd.util.PropertiesUtil
import org.apache.spark.sql.{DataFrameReader, SparkSession}

/**
  * Created by zhangrb on 2017/6/4.
  */
object HiveReader extends Session{

  def getHiveReader: DataFrameReader = {

    val prop: Properties = new Properties()
    PropertiesUtil.loadFromFile(prop, "algorithm.properties")
    println("url : " + prop.getProperty("sqlUrl"))

    val reader = sparkSession.read.format("jdbc")
      .option("url", prop.getProperty("sqlUrl").replace("\"",""))
      .option("user", prop.getProperty("sqlUser").replace("\"",""))
      .option("password", prop.getProperty("sqlPassword").replace("\"",""))
      .option("dbtable", prop.getProperty("sqlDbtable").replace("\"",""))
      .option("driver", prop.getProperty("sqlDriver").replace("\"",""))
    reader
  }

}
