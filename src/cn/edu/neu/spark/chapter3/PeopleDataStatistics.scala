package cn.edu.neu.spark.chapter3

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object PeopleDataStatistics {
  private val schemaString = "id,gender,height" //title
  def main(args: Array[String]): Unit = {
    
    ///--------------配置文件-------------------------
    if(args.length < 3){
      println("Usage:PeopleDataStatistics Parameter: ")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("People Data Statistics").setMaster(args(0))
    val sc = new SparkContext(conf)
    sc.addJar(args(2))
    ///-------------上面为常规的配置---------------------------- 
    
    val peopleData = sc.textFile(args(1))  //从HDFS上加载数据
    val sqlContext = new SQLContext(sc)    //在SparkContext的基础上创建spark-sql
    
    //this is used to implicity convert an RDD to a DataFrame
    ///------------ RDD -->  DataFrame --------
    import sqlContext.implicits._
    val schemaArray = schemaString.split(",")
    val schema = StructType(schemaArray.map(fieldName =>
      StructField(fieldName, StringType, true)))
    val rowRDD : RDD[Row] = peopleData.map(_.split(" ")).map(
        eachRow => Row(eachRow(0), eachRow(1), eachRow(2)))
    val peopleDF = sqlContext.createDataFrame(rowRDD, schema)
    
    peopleDF.registerTempTable("people")
    
    //get the male people whose height is more than 180
    val higherMale_180 = sqlContext.sql("select id, gender, height from people where height > 180 and gender = 'M'")
    println("Men whose height are more than 180 : " + higherMale_180.count()+" ")
    println("<Display #1>")
   
    //get the female people whose height is more than 170
    val higherFemale_170 = sqlContext.sql("select id, gender, height from people where height > 170 and gender = 'F'")
    println("Women whose height are more than 170 : " + higherFemale_170.count()+" ")
    println("<Display #2>")
   
    //Grouped the people by gender and count the number
    peopleDF.groupBy(peopleDF("gender")).count().show()
    println("People Count Grouped By Gender")
    println("<Display #3>")
    
    //Print height more than 210 and top 50 amd male
    peopleDF.filter(peopleDF("gender").equalTo("M")).filter(peopleDF("height") > 210).show(50)
    println("Men wohose height is more than 210")
    println("<Display #4>")
    
    //sort all height and take top 50
    peopleDF.sort(peopleDF("height")).show(50)
    peopleDF.sort(peopleDF("height").desc).take(50).foreach(println)
    println("Sorted the people by height in descend oreder, Show top 50 people")
    println("<Display #5>")
    
    //average male height
    peopleDF.filter(peopleDF("gender").equalTo("M")).select(peopleDF("height")).agg(Map("height" -> "avg")).show()
    println("The average height for men")
    println("<Display #6>")
    
    //female max height
    peopleDF.filter(peopleDF("gender").equalTo("F")).select(peopleDF("height")).agg(Map("height" -> "max")).show()
    println("The max height for female")
    println("<Display #7>")
  }
}