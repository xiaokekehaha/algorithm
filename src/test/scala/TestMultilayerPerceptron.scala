
import com.safe2345.utils.Session
import org.apache.spark.ml.Pipeline
import org.junit.Test
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.evaluationre.MulticlassClassificationEvaluatorReWrite
import org.apache.spark.ml.evaluationre.MulticlassClassificationEvaluatorReWrite
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}


/**
  * Created by zhangrb on 2017/6/21.
  */
class TestMultilayerPerceptron extends Session{

  @Test
  def multilayerPerceptron() : Unit = {

    val data = sparkSession.read.format("libsvm")
      .load("C:\\Users\\zhangrb\\Desktop\\foo.finish")

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
      .addGrid(trainer.layers,Array{Array[Int](33, 40,37, 2);Array[Int](33, 35,40, 2)})
//      .addGrid(trainer.solver, Array(1e-6))
      .addGrid(trainer.maxIter,Array(100,150,200))
      .addGrid(trainer.blockSize,Array(128,256))
      .addGrid(trainer.seed,Array(1234L))
      .build()
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator()
      .setMetricName("weightedPrecision"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(6)
    // train the model
    val model = cv.fit(data)

    // compute accuracy on the test set

    val dataTest = sparkSession.read.format("libsvm")
//      .load("C:\\Users\\zhangrb\\Desktop\\data\\split_test_feature.txtac")
      .load("C:\\Users\\zhangrb\\Desktop\\wenjain\\alive.alive")
//      .take(100)

    val result = model.transform(test)
    val predictionAndLabels = result.select("prediction", "label")
    predictionAndLabels.show(400)

//    val evaluatorf1 = new MulticlassClassificationEvaluatorReWrite()
//      .setMetricName("f1")

//    val evaluatoracc = new MulticlassClassificationEvaluatorReWrite()
//      .setMetricName("accuracy")

    val evaluatorrecall = new MulticlassClassificationEvaluatorReWrite()
      .setMetricName("weightedRecall")
    val evaluatorpre = new MulticlassClassificationEvaluatorReWrite()
      .setMetricName("weightedPrecision")


//    println("Test set f1 = " + evaluatorf1.evaluate(result))
//    println("Test set accuracy = " + evaluatoracc.evaluate(result))
    println("Test set weightedRecall = " + evaluatorrecall.evaluate(result))
    println("Test set weightedPrecision = " + evaluatorpre.evaluate(result))


    val resultOut = model.transform(dataTest)

//    resultOut.select("prediction", "label")
//      .filter("prediction = 1")
//      .rdd.coalesce(1).saveAsTextFile("C:\\Users\\zhangrb\\Desktop\\res\\mulac1")



  }


}
