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
      .load("C:\\Users\\zhangrb\\Desktop\\foo.finish")
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scFeatures")
      .setWithMean(false) //数据为稀疏矩阵，必须设置为false
      .setWithStd(true)
    val modelSc = scaler.fit(dataset)
    val data = modelSc.transform(dataset)

    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2), seed = 1234L)


    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("scFeatures")

    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(rf))

    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.maxBins, Array(50, 34, 100))
      .addGrid(rf.numTrees, Array(3, 5, 6, 7, 8, 10))
      .addGrid(rf.maxDepth, Array(3, 6, 9))
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
    predictions.select("label", "prediction").show(400)


    val evaluatorpre = new MulticlassClassificationEvaluator()
      .setMetricName("weightedPrecision")

    val weightedPrecision = evaluatorpre.evaluate(predictions)
    println("Test weightedPrecision = " + weightedPrecision)



  }
}
