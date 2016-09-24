package cn.edu.neu.spark.chapter1.test

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
    val file = sc.textFile(args(0), 2)

    val topRDD = file.map { line => line.toLowerCase() }
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map { case (a, b) => (b, a) }
      .sortByKey(false)
      .map { case (a, b) => (b, a) }
    //show results
    val topK = topRDD.collect().take(5)
    topK.foreach(println)
    topRDD.saveAsTextFile(args(1))
  }
}