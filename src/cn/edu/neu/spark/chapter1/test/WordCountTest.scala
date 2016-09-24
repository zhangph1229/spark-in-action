package cn.edu.neu.spark.chapter1.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
  *
  */
object WordCountTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage:WordCountTest Spark FileName");
      System.exit(1)
    }
    //set configuration
    val conf = new SparkConf().setAppName("Word Count Test").setMaster("spark://ubuntu1:7077")
    //start sparkcontext
    val sc = new SparkContext(conf)

    //load file from  hdfs
    val file = sc.textFile(args(0))
    //map reduce
    val wc = file.flatMap(words => words.split(" "))
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b);

    //show the results in the console
    println("Word Count results is : ")
    wc.collect().foreach(println)
    //save in hdfs
    wc.saveAsTextFile(args(1))

  }
}