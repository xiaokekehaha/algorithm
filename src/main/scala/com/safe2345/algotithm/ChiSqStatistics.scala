package com.safe2345.algotithm

import com.safe2345.hive.HiveReader._
import com.safe2345.utils.Session
import org.apache.spark.mllib.stat.Statistics._
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by zhangrb on 2017/6/20.
  */
class ChiSqStatistics extends Session{

  def chiSqStatistics(Args: String): Unit = {

    val dataMb = MLUtils.loadLibSVMFile(sc, "F:\\data.txt")
    val res = chiSqTest(dataMb)
    res.foreach(x => println(x))

  }

}
