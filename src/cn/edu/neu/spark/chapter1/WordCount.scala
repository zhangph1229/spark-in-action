package cn.edu.neu.spark.chapter1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object WordCount {
  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      println("Usage:WordCount Spark FileName")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Word Count").setMaster("spark://ubuntu1:7077")
    val sc = new SparkContext(conf)
    sc.addJar("file://F:/program/java/spark/jar/wordcount.jar")
    val wordCount = sc.textFile(args(0))
      .flatMap(x => x.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    //show the resultes
    println("Word Count program running results:")
    wordCount.collect().foreach(words => {
      val (key, value) = words
      println(key + "=" + value)
    })

    wordCount.saveAsTextFile(args(1))
    println("Word Count program running results are successfully saved.")
  }
}