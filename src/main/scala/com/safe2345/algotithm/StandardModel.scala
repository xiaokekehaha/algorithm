package com.safe2345.algotithm

import com.safe2345.utils.Session
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._


/**
  * Created by zhangrb on 2017/6/19.
  * z?score规范化，又叫零均值规范化
  *将某个特征向量（由所有样本某一个特征组成的向量）
  *进行标准化，使数据均值为0，方差为1。Spark中可以选择是带或者不带均值和方差
  */
class StandardModel extends Session{

  def stander(Args: Array[String]):Unit = {

    val dataSet = sparkSession.read.format("libsvm")
      .load("F:\\randomOne.txt")
    dataSet.foreach(x => println(x))

    val scaler=new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scFeatures")
      .setWithMean(false)//数据为稀疏矩阵，必须设置为false
     .setWithStd(true)
    val model=scaler.fit(dataSet)
    model.transform(dataSet)
//      .show(10,false)
      .show(10)



  }



  /**
    * Created by zhangrb on 2017/6/19.
    * 同样是对某一个特征操作，各特征值除以最大绝对值，
    * 因此缩放到[-1,1]之间。且不移动中心点。不会将稀疏矩阵变得稠密。
    * 例如一个叫长度的特征，有三个样本有此特征，特征向量为[-1000,100,10],
    * 最大绝对值为1000，因此转换为[-1000/1000,100/100,10/1000]=[-1,0.1,0.01]。
    * 因此如果最大绝对值是一个离群点，显然这种处理方式是很不合理的。
    */

  def maxAbs(Args: Array[String]):Unit = {

    val dataSet = sparkSession.read.format("libsvm")
      .load("F:\\randomOne.txt")
    dataSet.foreach(x => println(x))

    val maxAbsScalerModel=new MaxAbsScaler()
      .setInputCol("features")
      .setOutputCol("maxAbsScalerFeatures")
      .fit(dataSet)
    maxAbsScalerModel.transform(dataSet).show(10,false)
  }


  /**
    * Created by zhangrb on 2017/6/19.
    * VectorIndexer是对数据集特征向量中的类别（离散值）特征
    * （index categorical features categorical features ）进行编号。
    * 它能够自动判断那些特征是离散值型的特征，并对他们进行编号，
    * 具体做法是通过设置一个maxCategories，
    * 特征向量中某一个特征不重复取值个数小于maxCategories，
    * 则被重新编号为0～K（K<=maxCategories-1）。
    * 某一个特征不重复取值个数大于maxCategories，
    * 则该特征视为连续值，不会重新编号（不会发生任何改变）。
    */


  def vectorIndex(Args: Array[String]):Unit = {

    val data = sparkSession.read.format("libsvm")
      .load("F:\\data.txt")
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)
    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4
    // distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(0)
      .fit(data)


    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(4)


    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("predictedLabel", "label", "features", "probability").show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

    val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    println("Learned classification forest model:\n" + rfModel.toDebugString)

  }


}
