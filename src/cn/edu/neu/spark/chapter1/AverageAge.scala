package cn.edu.neu.spark.chapter1

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

    val file = sc.textFile(args(0), 5)
    val count = file.count()
    val AllAge = file.map(x => x.split(" ")(1)).map(age => Integer.parseInt(
      String.valueOf(age))).collect().reduce((a, b) => a + b)
    val averageAge = AllAge / count;

    //show results int console
    println("AllAge : " + AllAge + " Count : " + count)
    println("Average Age is : " + averageAge)
  }
}