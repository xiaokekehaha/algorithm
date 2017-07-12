import java.util.Properties

import com.safe2345.hbase.{HBaseClient, HbaseUtil}
import com.safe2345.hive.HiveReader._
import com.safe2345.utils.PropUtil
import com.safe2345.utils.SplitFeaturesUtil.{sc => _, sparkSession => _, _}
import jodd.util.PropertiesUtil
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics.chiSqTest

/**
  * Created by zhangrb on 2017/6/4.
  */
@Test
class TestConnect {
  @Test
  def testProp(): Unit = {
    val prop: Properties = new Properties()
    PropertiesUtil.loadFromFile(prop, "algorithm.properties")
    println("url1 : " + prop.getProperty("url"))
  }

  @Test
  def testLocalProp(): Unit = {
    val prop: Properties = PropUtil.getHiveProp()
    println("url2 : " + prop.getProperty("url"))
  }

  @Test
  def testSqlReader(): Unit = {
//    val prop: Properties = PropUtil.getHiveProp()

    val prop: Properties = new Properties()
    PropertiesUtil.loadFromFile(prop, "algorithm.properties")
    println("url : " + prop.getProperty("sqlUrl"))
    println("user", prop.getProperty("sqlUser"))
    println("password", prop.getProperty("sqlPassword"))
    println("dbtable", prop.getProperty("sqlDbtable"))
    println("driver", prop.getProperty("sqlDriver"))
    val reader = sparkSession.read.format("jdbc")
      .option("url", prop.getProperty("sqlUrl").replace("\"",""))
      .option("user", prop.getProperty("sqlUser").replace("\"",""))
      .option("password", prop.getProperty("sqlPassword").replace("\"",""))
      .option("dbtable", prop.getProperty("sqlDbtable").replace("\"",""))
      .option("driver", prop.getProperty("sqlDriver").replace("\"","")).load()
      reader.show()
  }

  @Test
  def testHiveReader(): Unit = {
    //    val prop: Properties = PropUtil.getHiveProp()

    val prop: Properties = new Properties()
    PropertiesUtil.loadFromFile(prop, "algorithm.properties")
    println("url : " + prop.getProperty("hiveUrl"))
    println("user", prop.getProperty("hiveUser"))
    println("password", prop.getProperty("hivePassword"))
    println("dbtable", prop.getProperty("hiveDbtable"))
    println("driver", prop.getProperty("hiveDriver"))
    val reader = sparkSession.read.format("jdbc")
      .option("url", prop.getProperty("hiveUrl").replace("\"",""))
      .option("user", prop.getProperty("hiveUser").replace("\"",""))
      .option("password", prop.getProperty("hivePassword").replace("\"",""))
      .option("dbtable", prop.getProperty("hiveDbtable").replace("\"",""))
      .option("driver", prop.getProperty("hiveDriver").replace("\"","")).load()
    reader.show()
  }




  @Test
  def testEquit(): Unit = {
    val prop: Properties = new Properties()
    PropertiesUtil.loadFromFile(prop, "algorithm.properties")
    if (prop.getProperty("sqlUrl").replace("\"","") == "jdbc:mysql://dw0:3306/dw_db"){
      println("equte")
      println(prop.getProperty("sqlUrl").replace("\"",""))
    } else {
      println("isEquit")
      println(prop.getProperty("sqlUrl").substring(1,prop.getProperty("sqlUrl").length -1))
      println("jdbc:mysql://dw0:3306/dw_db")
    }

  }


  @Test
  def testHbase(): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "master")
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "user")

  }

  @Test
  def hbaseConnect(): Unit = {
//    http://wuchong.me/blog/2015/04/06/spark-on-hbase-new-api/

    case class RawDataRecord(a:String, b: String)

    val sparkConf = new SparkConf()
      .setMaster("local").setAppName("HBaseTest")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create
    val tablename = "user_profile"
    conf.set("hbase.zookeeper.property.clientPort", "2181")
//    conf.set("hbase.zookeeper.master","node1:60000")
    conf.set("zookeeper.znode.parent", "/hbase/data") //根目录
//    conf.set("hbase.zookeeper.quorum",
//      "hadoop36.newqd.com,hadoop37.newqd.com,hadoop38.newqd.com") //DataNode的IP ,
    conf.set("hbase.zookeeper.quorum","172.16.19.4,172.16.19.22,172.16.19.44")
    conf.set(TableInputFormat.INPUT_TABLE, tablename)

    val connection= ConnectionFactory.createConnection(conf)
//    val s = connection.getTable(TableName)
//    println(s)

    val userTable = TableName.valueOf("user")
    val table = connection.getTable(userTable)


    val scan = new Scan
    val results = table.getScanner(scan)
    val s = results.toString
    println(s)

//    val g = new Get("id001".getBytes)
//    val result = table.get(g)
//    val value = Bytes.toString(result.getValue("basic".getBytes,"name".getBytes))
//    println("GET id001 :"+value)

//    从数据源获取数据
//    val hbaseRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],
//      classOf[ImmutableBytesWritable],classOf[Result])
//    hbaseRDD.foreach(x => println(x))

//    将数据映射为表  也就是将 RDD转化为 dataframe schema
//    val shop = hbaseRDD.map{r=>
//      val a = Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("customer_id")))
//      val b = Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("create_id")))
//      RawDataRecord(a,b)
//    }



    def x(s :String) : Unit = {

    }




//    HbaseUtil.scanRecord(connection,"uuid","browser","qmgr")
  }


  @Test
  def hbase() : Unit = {


//    val s = sparkSession
//        .read
//        .option(HBaseTableCatalog.tableCatalog,"cat")
//        .format("org.apache.spark.sql.execution.datasources.hbase")
//        .load()
//    s.write.option(
//      HBaseTableCatalog.tableCatalog,catalog, HBaseTableCatalog.newTable , "5"))
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .save()
  }

  @Test
  def round(): Unit = {
    for ( i <- 0 to 20) {
      val rand = Math.random()
      println(rand)
    }


  }

  @Test
  def hbseClient(): Unit = {

    val hbse = new HBaseClient("user_profile","172.16.19.4,172.16.19.22,172.16.19.44")
    hbse.scanBase().setBatch(1)
    val row = hbse.findByKey("000000206B276F99D11A71DDF1CEA865")
    println(row)
  }

  @Test
  def Hdfs() : Unit = {
//    sc.textFile("hdfs://dw1:8020/user/hive/warehouse/dm_db.db/dm_channel_inst_process/p_dt=2017-06-07/p_hours=14/000000_0").take(3).foreach(x => println(x))
    sc.textFile("hdfs://dw1:8020/ods/safe_click/20170607/00/172.16.20.156-2017-06-07-00.txt.snappy").take(3).foreach(x => println(x))
//    sc.textFile("hdfs://dw1:8020/user/hadoop*").take(2).foreach(println)
  }


  @Test
  def stander() : Unit = {
    val dataSet = sparkSession.read.format("libsvm")
      .load("F:\\randomOne.txt")
    dataSet.foreach(x => println(x))
    val scaler=new StandardScaler()
          .setInputCol("features")
          .setOutputCol("scFeatures")
          .setWithMean(false)//数据为稀疏矩阵，必须设置为false
         .setWithStd(true)
    val model=scaler.fit(dataSet)
    val data = model.transform(dataSet)
    //      .show(10,false)
    data.show(10)
  }


  @Test
  def kmeansTest(): Unit = {

      val dataSet = sparkSession.read.format("libsvm")
        .load("F:\\randomOne.txt")

      val scaler=new StandardScaler()
        .setInputCol("features")
        .setOutputCol("scFeatures")
        .setWithMean(false)//数据为稀疏矩阵，必须设置为false
        .setWithStd(true)
      val model=scaler.fit(dataSet)
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
  }

  @Test
  def randTree(): Unit = {

    val data = sparkSession.read.format("libsvm")
      .load("F:\\data.txt")
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)
    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4
    // distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)

    val slicer = new VectorSlicer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")


    slicer.setIndices(Array(0,2))
      .setNames(Array("f2","f3"))

    val quantile = new QuantileDiscretizer()
      .setInputCol("f2")
      .setOutputCol("qdCategory")
      .setNumBuckets(4)//设置分箱数
     .setRelativeError(0.1)//设置precision-控制相对误差
     .fit(data)
//     .transform(data)
//     .show(10,false)


    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(3)


    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, quantile, rf, labelConverter))

    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("predictedLabel", "label", "features", "probability").show(5)

  }

  @Test
  def chiSqSelector():Unit = {
    val data = sparkSession.read.format("libsvm")
      .load("F:\\data.txt")
    //学习并建立模型

    data.select("*").show(2)


    val selector = new ChiSqSelector()
          .setNumTopFeatures(1)
          .setFeaturesCol("features")
          .setLabelCol("label")
          .setOutputCol("selectFeatures")


    val model = selector.fit(data)


    // 计算
    val result = model.transform(data)
    println(selector.getLabelCol)
    println(selector.getFeaturesCol)


   println(s"ChiSqSelector output with top ${selector.getNumTopFeatures} features selected")
   result.show(false)

  }

  @Test
  def dataType(): Unit = {
    var m= 0
    val dataSet = sparkSession.read.format("libsvm")
      .load("C:\\Users\\zhangrb\\Desktop\\test1.test").rdd.map{
      m = m+1
      x => (m,x)
    }

    dataSet.foreach(x => println(x))
  }


}

