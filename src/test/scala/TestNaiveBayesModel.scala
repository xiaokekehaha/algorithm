import com.safe2345.utils.Session
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.junit.Test

/**
  * Created by zhangrb on 2017/6/21.
  */
class TestNaiveBayesModel extends Session{

  @Test
  def testNaiveBayesModel():Unit = {

    // Load the data stored in LIBSVM format as a DataFrame.
    val data = sparkSession.read.format("libsvm")
      .load("C:\\Users\\zhangrb\\Desktop\\foo.new_feature@20170626")

    val selector = new ChiSqSelector()
      .setNumTopFeatures(33)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setOutputCol("selectFeatures")

    // Split the data into training and test sets (30% held out for testing)
//    val Array(trainingData, testData) = data
//      .randomSplit(Array(0.7, 0.3), seed = 1234L)

    // Train a NaiveBayes model.
    val nb = new NaiveBayes()
//      .setSmoothing(0.01)
      .setFeaturesCol("features") //lr.smoothing
      .setLabelCol("label")
      .setProbabilityCol("probability")
      .fit(data)

    val paramGrid = new ParamGridBuilder()
      .addGrid(nb.smoothing, Array(0.1,0.05,0.01))
      .build()



    val pipeline = new Pipeline()
      .setStages(Array(selector, nb))

      val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator()  //import
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("weightedPrecision"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(20)



    val model = cv.fit(data)

    // Select example rows to display.
    val predictions = model.transform(data)
    predictions.show(400)

    // Select (prediction, true label) and compute test error
    val evaluatorf1 = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedPrecision")


    val evaluatoracc = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val evaluatorrecall = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")
    val evaluatorpre = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedPrecision")


    println("Test set f1 = " + evaluatorf1.evaluate(predictions))
    println("Test set accuracy = " + evaluatoracc.evaluate(predictions))
    println("Test set weightedRecall = " + evaluatorrecall.evaluate(predictions))
    println("Test set weightedPrecision = " + evaluatorpre.evaluate(predictions))


    val dataSet = sparkSession.read.format("libsvm")
      .load("C:\\Users\\zhangrb\\Desktop\\data\\split_test_feature.txtab")

    val result = model.transform(dataSet).select("prediction", "label")
      .filter("prediction = 1.0")
    //    result.show()

    result.rdd.coalesce(1)
      .saveAsTextFile("C:\\Users\\zhangrb\\Desktop\\res\\randab")


  }

}
