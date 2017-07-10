import com.safe2345.utils.Session
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{LabeledPoint, StandardScaler}
import org.apache.spark.ml.linalg.{Matrices, Vectors}
import org.junit.Test
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark
import org.apache.spark.rdd.RDD

/**
  * Created by zhangrb on 2017/6/27.
  */
class TestKeans extends Session{

  @Test
  def testKeans():Unit = {

    val srcRDD = sc.textFile("F:\\hdfs\\*").map(_.split("\t")).cache()

    val len = srcRDD.map(x =>x.length).take(1)(0)
    println(len)

//    featureMark(srcRDD,1)

    //初始化

//    var store = featureMark(srcRDD,1)

//    var j = 2
//    while (j <= len -1 ){
//      println(j)
//      featureMark(srcRDD,j).union(featureMark(srcRDD,1)).foreach(x => println(x))
//      println("444")
//      j = j+1
//    }

//     合并RDD
//    for (j <- 2 to len) {
//
//      store = featureMark(srcRDD,j).union(store)
//
//    }

//    //存储
//    store.saveAsTextFile("C:\\2")

      featureMark(srcRDD,1)
//      .union(featureMark(srcRDD,2))
//
//      .union(featureMark(srcRDD,3)).coalesce(1)
////        .foreach(x => println(x))
//
//      .union(featureMark(srcRDD,4))
//      .union(featureMark(srcRDD,5))
//      .union(featureMark(srcRDD,6))
//      .union(featureMark(srcRDD,7))
//      .union(featureMark(srcRDD,8))
//      .union(featureMark(srcRDD,9))
//      .union(featureMark(srcRDD,10))
//      .union(featureMark(srcRDD,11))
//      .union(featureMark(srcRDD,12))
//      .union(featureMark(srcRDD,13))
//      .union(featureMark(srcRDD,14))
//      .union(featureMark(srcRDD,15))
//      .union(featureMark(srcRDD,16))
        .coalesce(1).saveAsTextFile("C:\\88")


    /**
      * to get every feature's clusterCenters and return one RDD
      *@param  text RDD 切割后数据
      *@param featureOrder feature’s  order
      *
     */

    def featureMark(text:RDD[Array[String]], featureOrder:Int): RDD[String] = {
//      var m =0
      val src= text.map {
//              m = m +1
        x =>
          LabeledPoint(x(1).toDouble,
            Vectors.dense(Array(x(1).toDouble
//              x(2).toDouble
//              x(3).toDouble,
//              x(4).toDouble,
//              x(5).toDouble,
//              x(6).toDouble,
//              x(7).toDouble,
//              x(8).toDouble,
//              x(9).toDouble,
//              x(10).toDouble,
//              x(11).toDouble,
//              x(11).toDouble,
            )))

        //        Matrices.dense(1,1,Array(x(1).toDouble))
      }

      val dataset = sparkSession.createDataFrame(src)

      val scaler = new StandardScaler()
        .setInputCol("features")
        .setOutputCol("scFeatures")
        .setWithMean(false) //数据为稀疏矩阵，必须设置为false
        .setWithStd(true)
      val model = scaler.fit(dataset)
      val data = model.transform(dataset)
      // Trains a k-means model.
      val kmeans = new KMeans().setK(4).setSeed(1L).setMaxIter(10)
      val model1 = kmeans.fit(data)
      // Evaluate clustering by computing Within Set Sum of Squared Errors.
      val WSSSE = model1.computeCost(data)
      println(s"Within Set Sum of Squared Errors = $WSSSE")
      // Shows the result
      val cluster = model1.clusterCenters.sortBy(x => x(0))

      //存储格式
      var i = 0
      val clu = cluster.map(x => {
        i = i + 1
        "{\"tag_name\":" + featureOrder + ",\"tag_value\":" + i + ",\"codition\":" + x(0) + "}"
      })

      sc.parallelize(clu)
    }



  }
}
