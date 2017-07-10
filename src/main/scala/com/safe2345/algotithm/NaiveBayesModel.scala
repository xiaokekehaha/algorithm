package com.safe2345.algotithm

import com.safe2345.utils.Session
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

/**
  * Created by zhangrb on 2017/6/21.
  */
class NaiveBayesModel extends Session{

  // Load the data stored in LIBSVM format as a DataFrame.
  val data = sparkSession.read.format("libsvm")
    .load("F:\\data1.txt")

  // Split the data into training and test sets (30% held out for testing)
  val Array(trainingData, testData) = data
    .randomSplit(Array(0.7, 0.3), seed = 1234L)

  // Train a NaiveBayes model.
  val model = new NaiveBayes()
    .setSmoothing(0.01)
    .setFeaturesCol("features") //lr.smoothing
    .setLabelCol("label")
    .setProbabilityCol("probability")
    .fit(trainingData)

  // Select example rows to display.
  val predictions = model.transform(testData)
  predictions.show()

  // Select (prediction, true label) and compute test error
  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")
  val accuracy = evaluator.evaluate(predictions)
  println("Test set accuracy = " + accuracy)


}
