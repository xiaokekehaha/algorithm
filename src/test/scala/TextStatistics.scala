import com.safe2345.hive.HiveReader._
import org.apache.spark.mllib.util.MLUtils
//import org.apache.spark.ml.feature.LabeledPoint
//import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.Statistics._
import org.apache.spark.sql.Row
import org.junit.Test
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
//import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.stat.test.ChiSqTestResult
import org.apache.spark.rdd.RDD

/**
  * Created by zhangrb on 2017/6/20.
  */
class TextStatistics {


  @Test
  def chi(): Unit = {
    /**
      *方法、自由度、方法的统计量(卡方值)、p值（） 0.0.5
      * 做卡方检验会得到一个卡方值,在统计中有个叫卡方分布,
      * 通过计算出来的卡方值计算得到这个值对应的概率值,那就是P,
      * 它表示这种情况发生的概率大小,如果小于0.05或者小于0.01,
      * 表明这种情况发生的概率很低,就认为小概率事件发生了
      * ,就应该认为这种情况是不正常的,不应该相信这种情况,应该拒绝
      *
      * 卡方检验就是统计样本的实际观测值与理论推断值之间的偏离程度，
      * 实际观测值与理论推断值之间的偏离程度就决定卡方值的大小，
      * 卡方值越大，越不符合；卡方值越小，偏差越小，越趋于符合，
      * 若两个值完全相等时，卡方值就为0，表明理论值完全符合。
      */

    // val x1 = Vectors.dense(458.88,21.12)
//    val x2 = Vectors.dense(497.12,22.88)
//    val c1 = chiSqTest(x1,x2)
//
//    println(c1)

//    val data = sparkSession.read.format("libsvm")
//      .load("F:\\data.txt")


    val dataMb = MLUtils.loadLibSVMFile(sc, "C:\\Users\\zhangrb\\Desktop\\foo.new_feature@20170626")
    val res = chiSqTest(dataMb)
//    res.foreach(x => println(x))
//    println(res(0))
//    res.length
//    println(res.indices(res(0)))

      //标识是哪一个特征
    val len = res.length
    var i = 0
    res.map(x => {
      i = i + 1
      (x.pValue,i)
    })
      .filter(x => x._1 < 0.1)
//      .filter(x => x._1.pValue < 0.1)
//      .map(x => x.→(x.pValue)) //获取某一部分值
//      .map(x =>x._2)
//      .map(x =>x.pValue < 0.5)
     .foreach(x => println(x))



  }


}
