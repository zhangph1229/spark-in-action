package cn.edu.neu.spark.chapter1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object PeopleSexHeight {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage:PeopleSexHeight Spark FileName")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("People Sex And Height").setMaster("spark://ubuntu1:7077")
    val sc = new SparkContext(conf)
    sc.addJar("file://F:/program/java/spark/jar/people.jar")
    val file = sc.textFile(args(0))
    val count = file.count()

    //word count
    val sexCount = file.flatMap(x => x.split(" ")(1)).map(sex => (sex, 1)).reduceByKey(_ + _)
    println("sexCount:")
    sexCount.collect().foreach(sex => {
      val (k, v) = sex
      println(k + "=" + v)
    })

    val maleData = file.filter(x => x.contains("M"))
      .map(sex => sex.split(" ")(1) + " " + sex.split(" ")(2))
    val femaleData = file.filter(x => x.contains("F"))
      .map(sex => sex.split(" ")(1) + " " + sex.split(" ")(2))
    val maleHeight = maleData.map(height => height.split(" ")(1).toInt)
    val femaleHeight = femaleData.map(height => height.split(" ")(1).toInt)

    val maleLower = maleHeight.sortBy(height => height).first()
    val femaleLower = femaleHeight.sortBy(height => height).first()

    val maleHigher = maleHeight.sortBy(h => h).max()
    val femaleHigher = femaleHeight.sortBy(height => height).max()

    val maleTotalHeight = maleHeight.reduce((a, b) => a + b)
    val femaleTotalHeight = femaleHeight.reduce((a, b) => a + b)
    val maleAvg = maleTotalHeight / count
    val femaleAvg = femaleTotalHeight / count

    println("maleLower : " + maleLower + " maleHigher : " + maleHigher + " maleAvg : " + maleAvg)
    println("femaleLower : " + femaleLower + " femaleHigher" + femaleHigher + " femaleAvg : " + femaleAvg)
  }
}