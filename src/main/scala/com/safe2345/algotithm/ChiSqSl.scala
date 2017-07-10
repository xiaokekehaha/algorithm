package com.safe2345.algotithm

import com.safe2345.utils.Session
import org.apache.spark.ml.feature.ChiSqSelector

/**
  * Created by zhangrb on 2017/6/19.
  */
class ChiSqSl extends Session {

  def chiSqSelector(Args: String): Unit = {

    val data = sparkSession.read.format("libsvm")
      .load("F:\\data.txt")
    //学习并建立模型

    data.select("*").show(2)

    val selector = new ChiSqSelector()
      .setNumTopFeatures(1)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setOutputCol("selectFeatures")

    val model = selector.fit(data)


    // 计算
    val result = model.transform(data)

    println(s"ChiSqSelector output with top ${selector.getNumTopFeatures} features selected")
    result.show(false)

//    spark.close()


  }

}
