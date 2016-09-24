package cn.edu.neu.spark.practice

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def FILE_NAME : String = "word_count_results_"
  def main(args: Array[String]) : Unit = {
    if(args.length < 3){
      println("Usage:WordCount Parameters: MasterName InPutFileName OutPutFileName LocationJar")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Word Count").setMaster(args(0))
    val sc = new SparkContext(conf)
    sc.addJar(args(3))
    
    val wordCount = sc.textFile(args(1), 4)
                      .flatMap(line => line.split(" "))
                      .map(word => (word, 1))
                      .reduceByKey((a, b) => a + b);
    
    //use for debug
    wordCount.collect().foreach(println)
    
    //save as text file
    wordCount.saveAsTextFile(args(2)+System.currentTimeMillis())
    
    sc.stop()
  }
}