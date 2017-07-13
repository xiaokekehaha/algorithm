import com.safe2345.utils.Session
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.evaluationre.MulticlassClassificationEvaluatorReWrite
import org.apache.spark.ml.feature.{IndexToString, StandardScaler, StringIndexer, VectorIndexer}
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuningre.CrossValidatorReWrite
import org.junit.Test

/**
  * Created by zhangrb on 2017/6/20.
  */
class TestRandomForest extends Session{

  @Test
  def testRandomForest(): Unit = {

    // Load and parse the data file, converting it to a DataFrame.
    val dataset = sparkSession.read.format("libsvm")
      .load("C:\\Users\\zhangrb\\Desktop\\data\\chongzu\\part.fourth_test_user")
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scFeatures")
      .setWithMean(false) //数据为稀疏矩阵，必须设置为false
      .setWithStd(true)
    val modelSc = scaler.fit(dataset)
    val data = modelSc.transform(dataset)

    val Array(trainingData, testData) = data.randomSplit(Array(0.9, 0.1), seed = 1234L)


    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("scFeatures")

    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(rf))

//      .addGrid(rf.maxBins, Array(57, 60, 100))
//      .addGrid(rf.numTrees, Array(3, 5, 6, 7, 8, 10))
//      .addGrid(rf.maxDepth, Array(6, 8, 9))
    val paramGrid = new ParamGridBuilder()
        .addGrid(rf.maxBins, Array( 60, 80,100))
        .addGrid(rf.numTrees, Array(3, 6, 10))
        .addGrid(rf.maxDepth, Array(6, 9,15))
      .build()


    val cv = new CrossValidatorReWrite()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluatorReWrite()
        .setMetricName("weightedPrecision"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)

    // Train model. This also runs the indexers.
    val model = cv.fit(trainingData)

    println("xxxxxcccc")
    //
    //    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
//    predictions.select("prediction", "label")
//      .rdd.coalesce(1).saveAsTextFile("C:\\Users\\zhangrb\\Desktop\\data\\resTest\\first\\7")


    val evaluatorpre = new MulticlassClassificationEvaluator()
      .setMetricName("weightedPrecision")

    val evaluatorre = new MulticlassClassificationEvaluator()
      .setMetricName("weightedRecall")

    val weightedPrecision = evaluatorpre.evaluate(predictions)
    println("Test weightedPrecision = " + weightedPrecision)

    val weightedre = evaluatorre.evaluate(predictions)
    println("Test weightedRecall = " + weightedre)

    val dataSe = sparkSession.read.format("libsvm")
      .load("C:\\Users\\zhangrb\\Desktop\\test2.txt")
    val scalerSe = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scFeatures")
      .setWithMean(false) //数据为稀疏矩阵，必须设置为false
      .setWithStd(true)
    val modelS = scalerSe.fit(dataSe)
    val dataS = modelS.transform(dataSe)
    val res = model.transform(dataS).show(1500)

    predictions.select("prediction", "label")
      .rdd.coalesce(1).saveAsTextFile("C:\\Users\\zhangrb\\Desktop\\data\\resTest\\fourth\\40")


  }
}
