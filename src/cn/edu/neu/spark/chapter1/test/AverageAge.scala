package cn.edu.neu.spark.chapter1.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object AverageAge {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage:AverageAge Spark FileName")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Average Age").setMaster("spark://ubuntu1:7077")
    val sc = new SparkContext(conf)

    val file = sc.textFile(args(0), 2)
    val count = file.count()
    val allAge = file.map(line => line.split(" ")(1))
      .map(age => Integer.parseInt(String.valueOf(age)))
      .collect().reduce(_ + _)
    val averageAge = allAge / count

    //show results to console
    println("count = " + count)
    println("allAge = " + allAge)
    println("averageAge = " + averageAge)

  }
}