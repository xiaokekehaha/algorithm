package org.apache.spark.ml.tuningre


import scala.collection.JavaConverters._

import org.apache.spark.sql.{DataFrame, Dataset}


import java.util.{List => JList}
import com.github.fommil.netlib.F2jBLAS
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, CrossValidatorParams, ValidatorParams}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{Dataset, _}
import org.apache.spark.sql.types.StructType
import org.json4s.DefaultFormats

/**
  * Created by zhangrb on 2017/7/11.
  */
private[ml] trait CrossValidatorParamsReWrite extends ValidatorParams {
  /**
    * Param for number of folds for cross validation.  Must be &gt;= 2.
    * Default: 3
    *
    * @group param
    */
  val numFolds: IntParam = new IntParam(this, "numFolds",
    "number of folds for cross validation (>= 2)", ParamValidators.gtEq(2))

  /** @group getParam */
  def getNumFolds: Int = $(numFolds)

  setDefault(numFolds -> 3)
}

/**
  * K-fold cross validation performs model selection by splitting the dataset into a set of
  * non-overlapping randomly partitioned folds which are used as separate training and test datasets
  * e.g., with k=3 folds, K-fold cross validation will generate 3 (training, test) dataset pairs,
  * each of which uses 2/3 of the data for training and 1/3 for testing. Each fold is used as the
  * test set exactly once.
  */
@Since("1.2.0")
class CrossValidatorReWrite @Since("1.2.0")(@Since("1.4.0") override val uid: String)
  extends Estimator[CrossValidatorModelReWrite]
    with CrossValidatorParamsReWrite with MLWritable with Logging {

  @Since("1.2.0")
  def this() = this(Identifiable.randomUID("cv"))

  private val f2jBLAS = new F2jBLAS

  /** @group setParam */
  @Since("1.2.0")
  def setEstimator(value: Estimator[_]): this.type = set(estimator, value)

  /** @group setParam */
  @Since("1.2.0")
  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

  /** @group setParam */
  @Since("1.2.0")
  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

  /** @group setParam */
  @Since("1.2.0")
  def setNumFolds(value: Int): this.type = set(numFolds, value)

  /** @group setParam */
  @Since("2.0.0")
  def setSeed(value: Long): this.type = set(seed, value)

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): CrossValidatorModelReWrite = {
    val schema = dataset.schema
    transformSchema(schema, logging = true)
    val sparkSession = dataset.sparkSession
    val est = $(estimator)
    val eval = $(evaluator)
    val epm = $(estimatorParamMaps)
    val numModels = epm.length
    val metrics = new Array[Double](epm.length)
    val splits = MLUtils.kFold(dataset.toDF.rdd, $(numFolds), $(seed))
    splits.zipWithIndex.foreach { case ((training, validation), splitIndex) =>
      val trainingDataset = sparkSession.createDataFrame(training, schema).cache()
      val validationDataset = sparkSession.createDataFrame(validation, schema).cache()
      // multi-model training
      logDebug(s"Train split $splitIndex with multiple sets of parameters.")
      val models = est.fit(trainingDataset, epm).asInstanceOf[Seq[Model[_]]]
      trainingDataset.unpersist()
      var i = 0
      while (i < numModels) {
        // TODO: duplicate evaluator to take extra params from input
        val metric = eval.evaluate(models(i).transform(validationDataset, epm(i)))
        logDebug(s"Got metric $metric for model trained with ${epm(i)}.")
        metrics(i) += metric
        i += 1
      }
      validationDataset.unpersist()
    }
    f2jBLAS.dscal(numModels, 1.0 / $(numFolds), metrics, 1)
    logInfo(s"Average cross-validation metrics: ${metrics.toSeq}")
    val (bestMetric, bestIndex) =
      if (eval.isLargerBetter) metrics.zipWithIndex.maxBy(_._1)
      else metrics.zipWithIndex.minBy(_._1)
    logInfo(s"Best set of parameters:\n${epm(bestIndex)}")
    logInfo(s"Best cross-validation metric: $bestMetric.")
    val bestModel = est.fit(dataset, epm(bestIndex)).asInstanceOf[Model[_]]
    copyValues(new CrossValidatorModelReWrite(uid, bestModel, metrics).setParent(this))
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = transformSchemaImpl(schema)

  @Since("1.4.0")
  override def copy(extra: ParamMap): CrossValidatorReWrite = {
    val copied = defaultCopy(extra).asInstanceOf[CrossValidatorReWrite]
    if (copied.isDefined(estimator)) {
      copied.setEstimator(copied.getEstimator.copy(extra))
    }
    if (copied.isDefined(evaluator)) {
      copied.setEvaluator(copied.getEvaluator.copy(extra))
    }
    copied
  }

  // Currently, this only works if all [[Param]]s in [[estimatorParamMaps]] are simple types.
  // E.g., this may fail if a [[Param]] is an instance of an [[Estimator]].
  // However, this case should be unusual.
  @Since("1.6.0")
  override def write: MLWriter = new CrossValidatorReWrite.CrossValidatorWriter(this)
}

@Since("1.6.0")
object CrossValidatorReWrite extends MLReadable[CrossValidatorReWrite] {

  @Since("1.6.0")
  override def read: MLReader[CrossValidatorReWrite] = new CrossValidatorReader

  @Since("1.6.0")
  override def load(path: String): CrossValidatorReWrite = super.load(path)

    class CrossValidatorWriter(instance: CrossValidatorReWrite) extends MLWriter {

    ValidatorParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit =
      ValidatorParams.saveImpl(path, instance, sc)
  }

  private class CrossValidatorReader extends MLReader[CrossValidatorReWrite] {

    /** Checked against metadata when loading model */
    private val className = classOf[CrossValidatorReWrite].getName

    override def load(path: String): CrossValidatorReWrite = {
      implicit val format = DefaultFormats

      val (metadata, estimator, evaluator, estimatorParamMaps) =
        ValidatorParams.loadImpl(path, sc, className)
      val numFolds = (metadata.params \ "numFolds").extract[Int]
      val seed = (metadata.params \ "seed").extract[Long]
      new CrossValidatorReWrite(metadata.uid)
        .setEstimator(estimator)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(estimatorParamMaps)
        .setNumFolds(numFolds)
        .setSeed(seed)
    }
  }
}

/**
  * CrossValidatorModel contains the model with the highest average cross-validation
  * metric across folds and uses this model to transform input data. CrossValidatorModel
  * also tracks the metrics for each param map evaluated.
  *
  * @param bestModel The best model selected from k-fold cross validation.
  * @param avgMetrics Average cross-validation metrics for each paramMap in
  *                   `CrossValidator.estimatorParamMaps`, in the corresponding order.
  */
@Since("1.2.0")
class CrossValidatorModelReWrite private[ml](
                                        @Since("1.4.0") override val uid: String,
                                        @Since("1.2.0") val bestModel: Model[_],
                                        @Since("1.5.0") val avgMetrics: Array[Double])
  extends Model[CrossValidatorModelReWrite] with CrossValidatorParamsReWrite with MLWritable {

  /** A Python-friendly auxiliary constructor. */
  private[ml] def this(uid: String, bestModel: Model[_], avgMetrics: JList[Double]) = {
    this(uid, bestModel, avgMetrics.asScala.toArray)
  }

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    bestModel.transform(dataset)
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    bestModel.transformSchema(schema)
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): CrossValidatorModelReWrite = {
    val copied = new CrossValidatorModelReWrite(
      uid,
      bestModel.copy(extra).asInstanceOf[Model[_]],
      avgMetrics.clone())
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new CrossValidatorModelReWrite.CrossValidatorModelWriter(this)
}

@Since("1.6.0")
object CrossValidatorModelReWrite extends MLReadable[CrossValidatorModelReWrite] {

  @Since("1.6.0")
  override def read: MLReader[CrossValidatorModelReWrite] = new CrossValidatorModelReader

  @Since("1.6.0")
  override def load(path: String): CrossValidatorModelReWrite = super.load(path)


  class CrossValidatorModelWriter(instance: CrossValidatorModelReWrite) extends MLWriter {

    ValidatorParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit = {
      import org.json4s.JsonDSL._
      val extraMetadata = "avgMetrics" -> instance.avgMetrics.toSeq
      ValidatorParams.saveImpl(path, instance, sc, Some(extraMetadata))
      val bestModelPath = new Path(path, "bestModel").toString
      instance.bestModel.asInstanceOf[MLWritable].save(bestModelPath)
    }
  }

  private class CrossValidatorModelReader extends MLReader[CrossValidatorModelReWrite] {

    /** Checked against metadata when loading model */
    private val className = classOf[CrossValidatorModelReWrite].getName

    override def load(path: String): CrossValidatorModelReWrite = {
      implicit val format = DefaultFormats

      val (metadata, estimator, evaluator, estimatorParamMaps) =
        ValidatorParams.loadImpl(path, sc, className)
      val numFolds = (metadata.params \ "numFolds").extract[Int]
      val seed = (metadata.params \ "seed").extract[Long]
      val bestModelPath = new Path(path, "bestModel").toString
      val bestModel = DefaultParamsReader.loadParamsInstance[Model[_]](bestModelPath, sc)
      val avgMetrics = (metadata.metadata \ "avgMetrics").extract[Seq[Double]].toArray
      val model = new CrossValidatorModelReWrite(metadata.uid, bestModel, avgMetrics)
      model.set(model.estimator, estimator)
        .set(model.evaluator, evaluator)
        .set(model.estimatorParamMaps, estimatorParamMaps)
        .set(model.numFolds, numFolds)
        .set(model.seed, seed)
    }
  }
}
