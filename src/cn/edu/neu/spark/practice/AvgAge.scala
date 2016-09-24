package cn.edu.neu.spark.practice

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object AvgAge {
  def main(args : Array[String]) : Unit = {
    if(args.length < 3){
      println("Usage:AvgAge Parameters:  MasterName InputFileName AddJarName")
      System.exit(1)
    }
    
    val conf = new SparkConf().setAppName("AvgAge").setMaster(args(0))
    val sc = new SparkContext(conf)
    sc.addJar(args(2))
    
    val file = sc.textFile(args(1), 4)
    val count = file.count()
    val allAge = file.map(line => line.split(" ")(1))  // line => line.split(" ")(1).toInt 容易出现不适Int是报错的分先
                      .map(age => Integer.parseInt(String.valueOf(age)))
                      .collect().reduce((a, b) => a + b)
    
    val avgAge : Double = allAge.doubleValue()/count.doubleValue()
    println("count : " + count + " allAge : " + allAge)
    println("so the average age is : " + avgAge)
    
    sc.stop()
  }
}