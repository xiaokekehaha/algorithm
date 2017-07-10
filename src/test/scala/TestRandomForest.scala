import com.safe2345.utils.Session
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.junit.Test

/**
  * Created by zhangrb on 2017/6/20.
  */
class TestRandomForest extends Session{

  @Test
  def testRandomForest(): Unit = {

    // Load and parse the data file, converting it to a DataFrame.
    val data = sparkSession.read.format("libsvm")
      .load("C:\\Users\\zhangrb\\Desktop\\foo.detail").cache()
//      .load("C:\\Users\\zhangrb\\Desktop\\foo.detail").cache()

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2),seed = 1234L)
//    val labelIndexer = new StringIndexer()
//      .setInputCol("label")
//      .setOutputCol("indexedLabel")
//      .fit(data)


    println("xxxxx")

    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.

//    val featureIndexer = new VectorIndexer()
//      .setInputCol("features")
//      .setOutputCol("indexedFeatures")
//      .setMaxCategories(12)
//      .fit(data)



    // Split the data into training and test sets (30% held out for testing).


    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(3)
      .setMaxBins(50)
      .setMaxDepth(6)



    // Convert indexed labels back to original labels.
//    val labelConverter = new IndexToString()
//      .setInputCol("prediction")
//      .setOutputCol("predictedLabel")
//      .setLabels(labelIndexer.labels)

    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(rf))




    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainingData)

    println("xxxxxcccc")
//
//    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("label", "prediction").show(400)

//    predictions.select("label","predictedLabel")
//      .rdd.coalesce(1)
//      .saveAsTextFile("C:\\Users\\zhangrb\\Desktop\\data\\split_test_feature.txtaa")
    // Select (prediction, true label) and compute test error.
//    val evaluator = new MulticlassClassificationEvaluator()
//      .setLabelCol("indexedLabel")
//      .setPredictionCol("prediction")
//      .setMetricName("accuracy")
//    val accuracy = evaluator.evaluate(predictions)
//    println("Test Error = " + (1.0 - accuracy))

    val evaluatorf1 = new MulticlassClassificationEvaluator()
      .setMetricName("f1")


    val evaluatoracc = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    val evaluatorrecall = new MulticlassClassificationEvaluator()
      .setMetricName("weightedRecall")
    val evaluatorpre = new MulticlassClassificationEvaluator()
      .setMetricName("weightedPrecision")


    println("Test set f1 = " + evaluatorf1.evaluate(predictions))
    println("Test set accuracy = " + evaluatoracc.evaluate(predictions))
    println("Test set weightedRecall = " + evaluatorrecall.evaluate(predictions))
    println("Test set weightedPrecision = " + evaluatorpre.evaluate(predictions))



//
//    val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
//    println("Learned classification forest model:\n" + rfModel.toDebugString)

    val dataSet = sparkSession.read.format("libsvm")
//      .load("C:\\Users\\zhangrb\\Desktop\\predict.new_feature@20170626").cache()
      .load("C:\\Users\\zhangrb\\Desktop\\data\\split_test_feature.txtac").cache()

//
//    val featureIndexer1 = new VectorIndexer()
//      .setInputCol("features")
//      .setOutputCol("indexedFeatures")
//      .setMaxCategories(12)
//      .fit(dataSet)
    val result = model.transform(dataSet)
        .select("prediction", "label")
      .filter("prediction = 1.0")
//    result.rdd.foreach(x => println(x))

//    result.write
//      .format("com.databricks.spark.csv")
//  .option("header","true").save("C:\\Users\\zhangrb\\Desktop\\102")

      result.rdd.coalesce(1)
//        .map(_.getAs[String]("predictedLabel"))
//     .foreach(x=>println(x))
        .saveAsTextFile("C:\\Users\\zhangrb\\Desktop\\res\\randccc")
  }


}
