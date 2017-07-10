package com.safe2345.hive

import com.safe2345.utils.Session


/**
  * Created by zhangrb on 2017/6/5.
  */
object HiveWrite extends Session{


  def getHiveWrite(): Unit = {

    HiveReader.getHiveReader.load.write
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .save()
  }
}
