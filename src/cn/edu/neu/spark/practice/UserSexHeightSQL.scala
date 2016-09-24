package cn.edu.neu.spark.practice

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

case class User(userID : String, sex : String, height : Double)
object UserSexHeightSQL {
  def main(args : Array[String]) : Unit = {
    if(args.length < 0){
      println("Usage: UserSexHeightSQL Parametor: ")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("User Sex Height").setMaster("spark://ubuntu1:7077")
    val sc = new SparkContext(conf)
    sc.addJar("file://F:/program/java/spark/jar/user.jar")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    ///------使用反射的方式创建DataFrame--------------------------
    val userDF = sc.textFile("hdfs://ubuntu1:9000/user/zhangph/input/peopleData.txt")
                    .map(_.split(" "))
                    .map(line => User(line(0),line(1),line(2).toDouble)).toDF()
    userDF.registerTempTable("user")                
    
    //用SQL语句的方式
    val count_male_180 = sqlContext.sql("SELECT * FROM user WHERE height > 180 and sex = 'M'")
//    count_male_180.write.save("hdfs://ubuntu1:9000/user/zhangph/output/count_male_180")
    println("sex = 'M' and height > 180 --> count = " + count_male_180.count())
    println("<Display #01>")
    
    val count_female_170 = sqlContext.sql("SELECT userID,sex,height FROM user WHERE height > 170 and sex = 'F'")
//    count_female_170.write.save("hdfs://ubuntu1:9000/user/zhangph/output/count_female_170")
    println("sex = 'F' and height > 170 --> count = " + count_female_170.count())
    println("<Display #02>")
    
    //使用RDD转换的方式 select()
    val count_male_180_1 = userDF.select(userDF("userID"),userDF("sex").equalTo("M"),userDF("height") > 180)
//    count_male_180_1.write.save("hdfs://ubuntu1:9000/user/zhangph/output/count_male_180_1")
    println("sex = 'M' and height > 180 --> count = " + count_male_180_1.count())
    println("<Display #03>")
    
    val count_female_170_1 = userDF.select(userDF("userID"),userDF("sex").equalTo("F"),userDF("height") > 170)
//    count_female_170_1.write.save("hdfs://ubuntu1:9000/user/zhangph/output/count_female_170_1")
    println("sex = 'F' and height > 170 --> count = " + count_female_170_1.count())
    println("<Display #04>")
    
    //使用RDD转换的方式 filter()
    val count_male_180_2 = userDF.filter(userDF("sex").equalTo("M")).filter(userDF("height") > 180)
//    count_male_180_2.write.save("hdfs://ubuntu1:9000/user/zhangph/output/count_male_180_2")
    println("sex = 'M' and height > 180 --> count = " + count_male_180_2.count())
    println("<Display #05>")
    
    val count_female_170_2 = userDF.filter(userDF("sex").equalTo("F")).filter(userDF("height") > 170)
//    count_female_170_2.write.save("hdfs://ubuntu1:9000/user/zhangph/output/count_female_170_2")
    println("sex = 'F' and height > 170 --> count = " + count_female_170_2.count())
    println("<Display #06>")
    
    //sql
    val group_by_sex = sqlContext.sql("SELECT sex,count(userID) FROM user GROUP BY sex")
//    group_by_sex.write.save("hdfs://ubuntu1:9000/user/zhangph/output/count_male")
    println("group by sex" + group_by_sex)
    println("<Display #11>")
    //RDD --> DF
    val group_by_sex_1 = userDF.groupBy(userDF("sex")).count.show
    println("<Display #12>")
    
    //RDD --> DF
    val height_210 = userDF.filter(userDF("height") > 210)
//    height_210.write.save("hdfs://ubuntu1:9000/user/zhangph/output/height210")
    height_210.show()
    println("<Display #21>")
    //sql
    val height_210_1 = sqlContext.sql("SELECT * FROM user WHERE height > 210").show(50)
    println("<Display #22>")
    
    //sql
    val height_sort = sqlContext.sql("SELECT * FROM user ORDER BY height DESC").show(10)
    println("<Display #31>")
    //RDD 
    val height_sort_1 = userDF.sort(userDF("height").desc).take(10).foreach(println)
    println("<Display #32>")
    
    //sql 
    val male_avg_height = sqlContext.sql("SELECT AVG(sex) FROM user WHERE sex = 'M'").show()
    println("Display #41")
    //RDD
    val male_avg_height_1 = userDF.filter(userDF("sex").equalTo("M")).agg(Map("height" -> "avg")).show()
    println("Dispaly #42")
    val male_avg_height_2 = userDF.filter(userDF("sex").equalTo("M")).agg("height" -> "avg").show()
    println("Dispaly #43")
    
    println("game over!!!")
    
  }
}