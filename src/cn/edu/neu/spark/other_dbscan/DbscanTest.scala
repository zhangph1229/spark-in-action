package cn.edu.neu.spark.other_dbscan

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object DbscanTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 0) {
      println("Usage: DBScan Parameters : ")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("DBScan Test").setMaster("spark://ubuntu1:7077")
    val sc = new SparkContext(conf)
    sc.addJar("F://program/java/spark/jar/testDbscan.jar")
    //提取数据
    val data = sc.textFile("hdfs://ubuntu1:9000/user/zhangph/input/dbscan.txt")
      .map(_.split(","))
      .map(p => (p(2).trim.toDouble, p(3).trim.toDouble))
      .zipWithUniqueId()
      .map(x => (x._2,x._1))
      .cache
    val eps = 3.0
    val minPts = 5
    val cluster:Dbscan = new Dbscan(eps,minPts,data)
    val res = cluster.predict(data)
    res.foreach(println)
  }
}