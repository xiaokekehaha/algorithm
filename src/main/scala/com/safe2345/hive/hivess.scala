package com.safe2345.hive

import org.apache.spark.sql.SparkSession

/**
  * Created by zhangrb on 2017/6/5.
  */
object hivess {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark2JdbcDs")
      .getOrCreate()


    //Class.forName("org.apache.hive.jdbc.HiveDriver")
        val url = "jdbc:hive2://dw2:10000/temp_db"
        val user = "hadoop"
        val password = "hadoop"
        val dbtable = "low_frequency_user__point"
        val driver = "org.apache.hive.jdbc.HiveDriver"

//    val url = "jdbc:mysql://dw0:3306/dw_db"
//    val user = "dw_service"
//    val password = "dw_service_818"
//    val dbtable = "dw_monitor_field_define_tbl"
//    val driver = "com.mysql.jdbc.Driver"


//    "(select * from temp) as aaa"
    val DF = spark.read.format("jdbc")
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", dbtable)
      .option("driver", driver)
      .load()
    DF.show()


    //    spark.udf.register("strLen",(s: String) =>s.length())
    spark.udf.register("strLen", (s: String) => len(s))

    def len(s: String): Int = {
      s.length()
    }
    //    println(DF.count)


    //    DF.createOrReplaceTempView("ontime")

    spark.sql("select strLen('456')").show()

//    def withCatalog(cat: String): Unit = {
//      spark
//        .read
//        .option(HBaseTableCatalog.tableCatalog,"cat")
//        .format("org.apache.hadoop.hbase.spark")
//        .load()
//    }

//


  }


}
