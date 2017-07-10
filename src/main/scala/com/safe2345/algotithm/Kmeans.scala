package com.safe2345.algotithm


import com.safe2345.utils.Session
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.linalg.Vector

/**
  * Created by zhangrb on 2017/6/6.
  */
class Kmeans extends Session {

  def kmeanRank(args: Array[String]): Array[Vector] = {
    //    org.apache.spark.ml.linalg.Vector
    // Loads data.
    val dataSet = sparkSession.read.format("libsvm")
      .load("F:\\randomOne.txt")



    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scFeatures")
      .setWithMean(false) //数据为稀疏矩阵，必须设置为false
      .setWithStd(true)
    val model = scaler.fit(dataSet)
    val data = model.transform(dataSet)



    // Trains a k-means model.
    val kmeans = new KMeans().setK(5).setSeed(1L).setMaxIter(20)
    val model1 = kmeans.fit(data)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model1.computeCost(data)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model1.clusterCenters.sortBy(x => x(0)).foreach(println)

    model1.clusterCenters.sortBy(x => x(0))
  }


}
