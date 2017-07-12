
import com.safe2345.utils.Session
import org.apache.spark.ml.Pipeline
import org.junit.Test
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.evaluationre.MulticlassClassificationEvaluatorReWrite
import org.apache.spark.ml.evaluationre.MulticlassClassificationEvaluatorReWrite
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.tuningre.CrossValidatorReWrite
import org.apache.spark.mllib.evaluationre.MulticlassMetricsReWrite

import scala.collection.Map


/**
  * Created by zhangrb on 2017/6/21.
  */
class TestMultilayerPerceptron extends Session{

  @Test
  def multilayerPerceptron() : Unit = {

    val dataset = sparkSession.read.format("libsvm")
      .load("C:\\Users\\zhangrb\\Desktop\\foo.finish")
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scFeatures")
      .setWithMean(false) //数据为稀疏矩阵，必须设置为false
      .setWithStd(true)
    val modelSc = scaler.fit(dataset)
    val data = modelSc.transform(dataset)
    // Split the data into train and test
    val splits = data.randomSplit(Array(0.8, 0.2), seed = 1234L)
    val train = splits(0)
    val test = splits(1)

    // specify layers for the neural network:
    // input layer of size 4 (features), two intermediate of size 5 and 4
    // and output of size 3 (classes)
//    val layers = Array[Int](4, 5, 4, 3)
    val layers = Array[Int](33, 40,37, 2)

    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
//      .setBlockSize(128)
//      .setSeed(1234L)
//      .setMaxIter(100)

    val pipeline = new Pipeline()
      .setStages(Array(trainer))

    val paramGrid = new ParamGridBuilder()
      .addGrid(trainer.layers,Array{Array[Int](33, 40,37, 2);
        Array[Int](33, 35,40, 2)})
//      .addGrid(trainer.solver, Array(1e-6))
//      .addGrid(trainer.maxIter,Array(100,150,200))
//      .addGrid(trainer.blockSize,Array(128,256))
//      .addGrid(trainer.seed,Array(1234L))
      .build()
    val cv = new CrossValidatorReWrite()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluatorReWrite()
      .setMetricName("weightedPrecision"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)
    // train the model
    val model = cv.fit(train).bestModel


    // compute accuracy on the test set

//    val dataTest = sparkSession.read.format("libsvm")
//      .take(100)

    val result = model.transform(test)
    result.select("prediction", "label").show(500)
    val predictionAndLabels = result.select("prediction", "label")

//      .rdd.map(x => (x(0).toString.toDouble,x(1).toString.toDouble))
//    println("end")
////    predictionAndLabels.show(400)
////    val mul = new MulticlassMetricsReWrite(predictionAndLabels)
//
//    val tpByClass: Map[Double, Int] = predictionAndLabels
//      .map { case (prediction, label) =>
//        (label, if (label == prediction) 1 else 0)
//      }.reduceByKey(_ + _)
//      .collectAsMap()
////      .foreach(x => println(x))
//    println("end")
//    val fpByClass: Map[Double, Int] =predictionAndLabels
//      .map { case (prediction, label) =>
//        (prediction, if (prediction != label) 1 else 0)
//      }.reduceByKey(_ + _)
//      .collectAsMap()
////      .foreach(x => println(x))
//
//
//    lazy val labelCountByClass: Map[Double, Long] =
//      predictionAndLabels.values.countByValue()
//    labelCountByClass.foreach(x => println(x))
//    lazy val labelCount: Long =
//      labelCountByClass.values.sum
//
//    def precision(label: Double): Double = {
//      println("label",label)
//      val tp = tpByClass(label)
//      println("tp",tp)
//      val fp = fpByClass.getOrElse(label, 0)
//      println("fp",fp)
//      if (tp + fp == 0) 0 else tp.toDouble / (tp + fp)
//    }
//
//
//    lazy val weightedPrecision: Double = labelCountByClass.map { case (category, count) =>
//      if (category == 1) precision(category)* count.toDouble / count.toDouble else 0
//    }.sum
//    println("weightedPrecision",weightedPrecision)

//    val evaluatorf1 = new MulticlassClassificationEvaluatorReWrite()
//      .setMetricName("f1")

//    val evaluatoracc = new MulticlassClassificationEvaluatorReWrite()
//      .setMetricName("accuracy")

//    val evaluatorrecall = new MulticlassClassificationEvaluatorReWrite()
//      .setMetricName("weightedRecall")
    val evaluatorpre = new MulticlassClassificationEvaluatorReWrite()
      .setMetricName("weightedPrecision")


//    println("Test set f1 = " + evaluatorf1.evaluate(result))
//    println("Test set accuracy = " + evaluatoracc.evaluate(result))
//    println("Test set weightedRecall = " + evaluatorrecall.evaluate(result))
    println("Test set weightedPrecision = " + evaluatorpre.evaluate(result))
//

//    val resultOut = model.transform(dataTest)

//    resultOut.select("prediction", "label").show()
//      .filter("prediction = 1")
//      .rdd.coalesce(1).saveAsTextFile("C:\\Users\\zhangrb\\Desktop\\res\\mulac1")

    val dataSet = sparkSession.read.format("libsvm")
      .load("C:\\Users\\zhangrb\\Desktop\\test1.test")

    println("dataSet")
    val result1 = model.transform(dataSet)
    result1.select("*").show()

  }


}
