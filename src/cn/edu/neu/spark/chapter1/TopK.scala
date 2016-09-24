package cn.edu.neu.spark.chapter1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object TopK {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage:TopK Spark FileName")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Top K").setMaster("spark://ubuntu1:7077")
    val sc = new SparkContext(conf)
//    sc.addJar("file://F:/program/java/spark/jar/topk.jar")
    val file = sc.textFile(args(0))
    val wordCount = file.map(line => (line.toLowerCase(), 1)).reduceByKey(_ + _)
    val sortData = wordCount.map { case (a, b) => (b, a) }.sortByKey(false)
    val takeValue = sortData.take(args(1).toInt).map { case (b, a) => (a, b) }

    //show results 
    takeValue.foreach(println)
    //    takeValue.foreach(res =>{
    //      val (k, v) = res
    //      println(v + "=" +k)
    //    })
    sc.stop()
  }
}