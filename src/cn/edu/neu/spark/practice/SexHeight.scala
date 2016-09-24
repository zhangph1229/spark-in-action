package cn.edu.neu.spark.practice

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SexHeight {
  def main(args : Array[String]) : Unit = {
    if(args.length < 3) {
      println("Usage:SexHeight Paramter : MasterName InputFileName AddJarsName")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Sex Height").setMaster(args(0))
    val sc = new SparkContext(conf)
    sc.addJar(args(2))
    
    val file = sc.textFile(args(1), 5)
    
    //filter sex
    val maleFile = file.filter(line => line.contains("M")).map(line => (line.split(" ")(1) + " " + line.split(" ")(2)))
    val femaleFile = file.filter(line => line.contains("F")).map(line => (line.split(" ")(1) + " " + line.split(" ")(2)))
    
    val maleSortHeight = maleFile.map(h => h.split(" ")(1).toFloat).collect().sortBy(h => h)
    val femaleSortHeight = femaleFile.map(h => h.split(" ")(1).toFloat).collect().sortBy(h => h)
    
    val maleLow = maleSortHeight.min
    val femaleLow = femaleSortHeight.min 
    val maleHigh = maleSortHeight.max 
    val femaleHigh = femaleSortHeight.max 
    
    //average
    val maleAvg = maleFile.map(h => h.split(" ")(1).toFloat).collect().reduce(_+_)/maleFile.count()
    val femaleAvg = femaleFile.map(h => h.split(" ")(1).toFloat).collect().reduce(_+_)/femaleFile.count()
    val avg = (maleAvg + femaleAvg) / 2
    
    println("maleLow = " + maleLow + " maleHigh = " + maleHigh)
    println("femaleLow = " + femaleLow + " femaleHigh = " + femaleHigh)
    println("avg = " + avg + " maleAvg = " + maleAvg + " femaleAvg = " + femaleAvg)
    
    sc.stop()
  }
}