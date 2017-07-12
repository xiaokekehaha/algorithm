import org.junit.Test
import com.safe2345.utils.Session
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.evaluationre.MulticlassClassificationEvaluatorReWrite
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuningre.CrossValidatorReWrite

/**
  * Created by zhangrb on 2017/7/12.
  */
class TestRandomForestOrigin extends Session{

  @Test
  def testRandomForestOrigin(): Unit = {

    // Load and parse the data file, converting it to a DataFrame.
    val dataOrigin = sparkSession.read.format("libsvm")
      .load("C:\\Users\\zhangrb\\Desktop\\foo.finish")

    val dataSet = sparkSession.read.format("libsvm")
      .load("C:\\Users\\zhangrb\\Desktop\\test1.test")

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scFeatures")
      .setWithMean(false) //数据为稀疏矩阵，必须设置为false
      .setWithStd(true)

    val modelSc = scaler.fit(dataOrigin.union(dataSet))
    val data = modelSc.transform(dataOrigin.union(dataSet))
    val tra = data.filter("label <= 1")
    val pre = data.filter("label > 1")


    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)


    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("scFeatures")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(10)
      .fit(data)

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = tra.randomSplit(Array(0.8, 0.2),1234L)

    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(5)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline()
//      .setStages(Array(rf))
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.maxBins, Array(50, 34, 100))//
      .addGrid(rf.numTrees, Array(3, 5, 6, 7, 8, 10)) //
      .addGrid(rf.maxDepth, Array(3, 4, 5, 6, 9))
      .build()


    val cv = new CrossValidatorReWrite()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluatorReWrite()
        .setMetricName("weightedPrecision"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)

    // Train model. This also runs the indexers.
    val model = cv.fit(trainingData).bestModel

    println("1111")

    // Make predictions.
    val predictions = model.transform(testData)

    println("22222")
    // Select example rows to display.
    predictions.select("prediction", "label", "features").show(500)
//    predictions.select("predictedLabel", "label", "features").show(5)

    println("33333")
    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluatorReWrite()
//      .setLabelCol("indexedLabel")
//      .setPredictionCol("prediction")
      .setMetricName("weightedPrecision")

    val weightedPrecision = evaluator.evaluate(predictions)
    println("Test weightedPrecision = " + weightedPrecision)


    val result1 = model.transform(pre)


    result1.select("*").show(500)


  }

}
