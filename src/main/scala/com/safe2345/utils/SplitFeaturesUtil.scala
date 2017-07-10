package com.safe2345.utils

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

/**
  * Created by zhangrb on 2017/6/19.
  */
object SplitFeaturesUtil extends Session{

  def splitFeaturesUtil(data:Dataset[_]) : Unit = {

    //为特征数组设置属性名（字段名），分别为f1 f2 f3
    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f1", "f2").map(defaultAttr.withName)

    val attrGroup = new AttributeGroup("features",
      attrs.asInstanceOf[Array[Attribute]])

    val dataSet = sparkSession.createDataFrame(data.select("features").rdd,
      StructType(Array(attrGroup.toStructField())))
    val dataSetName = dataSet.select("*")


    val slicer = new VectorSlicer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")


    val dataSe = slicer
//      .setIndices(Array(0))  //通过索引获取部分feature
      .setNames(Array("f1"))   //通过属性名获取部分feature
    dataSe.transform(dataSetName).select("*")
//      .show()
  }

}
