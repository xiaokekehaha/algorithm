package org.apache.spark.ml.evaluationre

import org.apache.spark.annotation.Since
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.{Param, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable, SchemaUtils}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.evaluationre.MulticlassMetricsReWrite
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
  * Created by zhangrb on 2017/7/10.
  */
class MulticlassClassificationEvaluatorReWrite @Since("1.5.0") (@Since("1.5.0") override val uid: String)
  extends Evaluator with HasPredictionCol with HasLabelCol with DefaultParamsWritable {

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("mcEval"))

  /**
    * param for metric name in evaluation (supports `"f1"` (default), `"weightedPrecision"`,
    * `"weightedRecall"`, `"accuracy"`)
    * @group param
    */
  @Since("1.5.0")
  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("f1", "weightedPrecision",
      "weightedRecall", "accuracy"))
    new Param(this, "metricName", "metric name in evaluation " +
      "(f1|weightedPrecision|weightedRecall|accuracy)", allowedParams)
  }

  /** @group getParam */
  @Since("1.5.0")
  def getMetricName: String = $(metricName)

  /** @group setParam */
  @Since("1.5.0")
  def setMetricName(value: String): this.type = set(metricName, value)

  /** @group setParam */
  @Since("1.5.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  setDefault(metricName -> "f1")

  @Since("2.0.0")
  override def evaluate(dataset: Dataset[_]): Double = {
    val schema = dataset.schema
    SchemaUtils.checkColumnType(schema, $(predictionCol), DoubleType)
    SchemaUtils.checkNumericType(schema, $(labelCol))

    val predictionAndLabels =
      dataset.select(col($(predictionCol)), col($(labelCol)).cast(DoubleType)).rdd.map {
        case Row(prediction: Double, label: Double) => (prediction, label)
      }
    val metrics = new MulticlassMetricsReWrite(predictionAndLabels)
    val metric = $(metricName) match {
      case "f1" => metrics.weightedFMeasure
      case "weightedPrecision" => metrics.weightedPrecision
      case "weightedRecall" => metrics.weightedRecall
      case "accuracy" => metrics.accuracy
    }
    metric
  }

  @Since("1.5.0")
  override def isLargerBetter: Boolean = true

  @Since("1.5.0")
  override def copy(extra: ParamMap): MulticlassClassificationEvaluatorReWrite = defaultCopy(extra)
}

@Since("1.6.0")
object MulticlassClassificationEvaluatorReWrite
  extends DefaultParamsReadable[MulticlassClassificationEvaluatorReWrite] {

  @Since("1.6.0")
  override def load(path: String): MulticlassClassificationEvaluatorReWrite = super.load(path)


}
