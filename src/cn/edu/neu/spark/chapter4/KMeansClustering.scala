package cn.edu.neu.spark.chapter4

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeansModel
import java.io.FileWriter

object KMeansClustering {
  def main(args: Array[String]): Unit = {
    if(args.length < 6){
      println("Usage:KMeans Clustering Parameters: master traindata  numCluster  numIter  runTimes testdata")
      System.exit(1)
    }
    //配置
    val conf = new SparkConf().setAppName("KMeans").setMaster(args(0))
    val sc = new SparkContext(conf)
//    sc.addJar("file://F:/program/java/spark/jar/kmeans.jar")
    
    ///Channel,Region,Fresh,Milk,Grocery,Frozen,Detergents_Paper,Delicassen
    val dataForTrain = sc.textFile(args(1))
    val parsedTrainData = dataForTrain.filter(!isTitleLine(_))
      .map(line => {
        Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
      }).persist()
    
    //get parameters
    val numCluster = args(2).toInt
    val numIter = args(3).toInt
    val runTimes = args(4).toInt
    var clusterIndex : Int = 0
    
    val sb = new StringBuilder();
    sb.append("-------------------------------------\n\t")
    //select K
    val ks:Array[Int] = Array(3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
    var min = 10000.0
    var k = 20
    ks.foreach(cluster =>{
      val model : KMeansModel = KMeans.train(parsedTrainData,cluster, 10, 1)
      val ssd : Double = model.computeCost(parsedTrainData)  //计算成本
//      if(min > ssd){
//        min = ssd
//        k = cluster
//      }
      println("sum of squared distandes of pointes to their nearest center when k = " + cluster + "-->" + ssd)
      sb.append("sum of squared distandes of pointes to their nearest center when k = " + cluster + "-->" + ssd)
      sb.append(System.getProperty("line.separator"))
    })
      sb.append("----------------------------------------\n\t")
      println(sb.toString())
    //train data
    val clusters:KMeansModel = KMeans.train(parsedTrainData, numCluster, numIter, runTimes)
    
    println("Cluster Cnters Information Overview:")
    clusters.clusterCenters.foreach(x => {
      println("Center Point of Cluster" + clusterIndex + ":" + x)
      clusterIndex += 1
    })
    
    //test data
    val dataForTest = sc.textFile(args(5))
    val parsedTestData = dataForTest.filter(!isTitleLine(_)).map(line => {
      Vectors.dense(line.split(",").map(_.trim()).filter(!"".equals(_)).map(_.toDouble))
    }).cache()
    parsedTestData.collect().foreach(line => {
      val predictedClusterIndex : Int = clusters.predict(line)
      println("The data " + line.toString() + " belongs to cluster " + predictedClusterIndex)
    })
    println("Spark MLlib K-means clustering test finished.")
  }

  def isTitleLine(arg: String) : Boolean= {
    if(arg != null && arg.contains("Channel")) true
    else false
  }
}