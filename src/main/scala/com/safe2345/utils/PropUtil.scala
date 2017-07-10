package com.safe2345.utils

import java.io.FileInputStream
import java.util.Properties

/**
  * Created by zhangrb on 2017/6/4.
  */
object PropUtil {
  def getHiveProp(): Properties = {
    val prop = new Properties()
    prop.load(new FileInputStream("algorithm.properties"))
    prop
  }
}
