import com.safe2345.utils.Session
import org.junit.Test
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
//import org.apache.spark.ml.evaluationre.MulticlassClassificationEvaluatorReWrite
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

/**
  * Created by zhangrb on 2017/6/21.
  */
class TestGradientBoostedTree extends Session{

  @Test
  def testGradientBoostedTree(): Unit = {

    // Load and parse the data file, converting it to a DataFrame.
    val data = sparkSession.read.format("libsvm")
      .load("C:\\Users\\zhangrb\\Desktop\\sample.finish").cache()


    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)
    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(12)
      .fit(data)

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))

    // Train a GBT model.
    val gbt = new GBTClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(10)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and GBT in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))

    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("predictedLabel", "label").show(400)

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
//    val accuracy = evaluator.evaluate(predictions)
//    println("Test Error = " + (1.0 - accuracy))

//    val gbtModel = model.stages(2).asInstanceOf[GBTClassificationModel]
//    println("Learned classification GBT model:\n" + gbtModel.toDebugString)


    val dataSet = sparkSession.read.format("libsvm")
      .load("C:\\Users\\zhangrb\\Desktop\\data\\split_test_feature.txtaa")
    dataSet.select("*").show(5)


    val labelIndexer1 = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(dataSet)
    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer1 = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(12)
      .fit(dataSet)

    val result = model.transform(dataSet)
      .select("predictedLabel", "label")
    result.select("*").show(5)
//      .filter("predictedLabel = 1.0")
    //    result.show()

    result.rdd.foreach(x=> println(x))
//      .coalesce(1)
//      .saveAsTextFile("C:\\Users\\zhangrb\\Desktop\\66667")
  }

}
