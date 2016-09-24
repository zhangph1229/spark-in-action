package cn.edu.neu.spark.practice

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable._

object ReverseSort {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage : ReverseSort Parameter: MasterName InputFileName AddJarName ")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Reverse Sort").setMaster(args(0))
    val sc = new SparkContext(conf)
    sc.addJar(args(2))

    val file = sc.textFile(args(1), 4)
    //    val splitFile = file.map(_.split(" ")).map(item => (item(0), item.tail))
    //      .map {
    //        case (key, values) =>
    //          (key, values.foreach { v => v })
    //      }
    //    //only for debug
    //    println("split file for debug")
    //    splitFile.foreach(println)
    //
    //    val distinctFile = splitFile.map { case (k, v) => (v, k) }.groupByKey().distinct()
    //    //debug
    //    distinctFile.foreach(println)
    //    val sortFile = distinctFile.sortByKey()
    //
    //    //show results
    //    println("reverse sort results is : ")
    //    sortFile.foreach(println)
    //
    val words = file.map(line => line.split("\t")).map(item => (item(0), item(1))).flatMap(file => {
      val list = new LinkedList[(String, String)]
      val words = file._2.split(" ").iterator
      while (words.hasNext) {
        list + words.next()
      }
      list
    }).distinct()
    val res = words.map(word => (word._2, word._1)).reduceByKey((a, b) => (a + "\t" + b))
    res.collect().foreach(println)
    
//    res.saveAsTextFile(args(3))
    
    sc.stop()
  }
}