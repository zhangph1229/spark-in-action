package cn.edu.neu.spark.practice

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.DAGScheduler

object TopK {
  def main(args : Array[String]) : Unit = {
    if(args.length < 3){
      println("Usage:TopK Parameter: MasterName InputFileName AddJarsName K")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Top K").setMaster(args(0))
    val sc = new SparkContext(conf)
    sc.addJar(args(2))
    val file = sc.textFile(args(1))
    val sortFile = file.map(line => (line.toLowerCase(), 1)).reduceByKey(_+_)
                       .map{case(k, v) => (v, k)}.sortByKey(false)
                       .map{case(v, k) => (k, v)}
    val topk = sortFile.collect().take(Integer.parseInt(String.valueOf(args(3))))
    
    topk.foreach(println)
    sc.stop()
  }
}