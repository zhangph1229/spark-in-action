package cn.edu.neu.spark.chapter1.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object PeopleSexHeight {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage:PeopleSexHeight Spark FileName")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("People Sex Height").setMaster("spark://ubuntu1:7077")
    val sc = new SparkContext(conf)

    val file = sc.textFile(args(0), 2)
    val maleFile = file.filter(sex => sex.contains("M"))
      .map(file => file.split(" ")(1) + " " + file.split(" ")(2))
    val femaleFile = file.filter { sex => sex.contains("F") }
      .map(file => file.split(" ")(1) + " " + file.split(" ")(2))

    val maleLowest = maleFile.map(height => height.split(" ")(1).toInt).sortBy(height => height).min()
    val maleHighest = maleFile.map(height => height.split(" ")(1).toInt).sortBy(height => height).max()
    val femaleLowest = femaleFile.map(height => height.split(" ")(1).toInt).sortBy(height => height).min()
    val femaleHighest = femaleFile.map(height => height.split(" ")(1).toInt).sortBy(height => height).max()

    val averageHeight = file.map(line => line.split(" ")(2).toInt).reduce(_ + _) / file.count()
    val maleAverageHeight = maleFile.map(height => height.split(" ")(1).toInt).reduce(_ + _)
    val femaleAverageHeight = femaleFile.map(height => height.split(" ")(1).toInt).reduce(_ + _)

    //show results 
    println("male - " + "low:" + maleLowest + " - " + "high:" + maleHighest)
    println("female - " + "low:" + femaleLowest + " - " + "high:" + femaleHighest)
    println("average - " + "all: " + averageHeight + " - " + "maleAverageHeight:" + maleAverageHeight
      + " - femaleAverageHeight:" + femaleAverageHeight)
  }
}