package com.safe2345.algotithm

import com.safe2345.hive.HiveReader

/**
  * Created by zhangrb on 2017/6/4.
  */
class Check {
  val reader = HiveReader.getHiveReader
  reader.load()
}
