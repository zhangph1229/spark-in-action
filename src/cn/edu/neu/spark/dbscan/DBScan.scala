package cn.edu.neu.spark.dbscan

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import breeze.numerics._
import nak.cluster._
import nak.cluster.GDBSCAN._
import breeze.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.Vectors
import breeze.linalg.DenseVector

object DBScan {
  def main(args: Array[String]): Unit = {
    if (args.length < 0) {
      println("Usage: DBScan Parameters : ")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("DBScan").setMaster("spark://ubuntu1:7077")
    val sc = new SparkContext(conf)
    sc.addJar("F://program/java/spark/jar/dbscan.jar")
    //提取数据
    val sourceData = sc.textFile("hdfs://ubuntu1:9000/user/zhangph/input/dbscan.txt")
    println("文件加载成功！！！！！")
    Thread.sleep(2000)
    val usage = sourceData.map(_.split(",")).map(row => {
      (row(2).toDouble+","+row(3).toDouble)
    }).map(s => DenseVector(s.split(',').map(_.toDouble))).cache()

    val data = DenseMatrix(
                (0.9, 1.0),
                (1.0, 1.0),
                (1.0, 1.1),
                (5.0, 5.0), // NOISE
          
                (15.0, 15.0),
                (15.0, 14.1),
                (15.3, 15.0)
              );
//    dbscan(usage);
    println("dbscan over")
  }

  def dbscan(v : breeze.linalg.DenseMatrix[Double])  = {
    val gdbscan = new GDBSCAN(
      DBSCAN.getNeighbours(epsilon = 1, distance = Kmeans.euclideanDistance),
      DBSCAN.isCorePoint(minPoints = 2))
    val clusters = gdbscan cluster v
    val clusterPoints = clusters.map(_.points.map(_.value.toArray))
    println("res add is "+clusterPoints.toString)
   
    println("---------------------")
    clusterPoints.foreach(line => {
      line.foreach(value => {
        value.foreach(v => print(v + " "))
        println()
      })
      println("-------------------")
    })
  }
}