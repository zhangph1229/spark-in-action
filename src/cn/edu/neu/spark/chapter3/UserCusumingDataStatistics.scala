package cn.edu.neu.spark.chapter3

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel

object UserCusumingDataStatistics {
  def main(args : Array[String]) : Unit = {
    if(args.length < 2){
      println("Usage:PeopleDataStatistics Parameter: ")
      System.exit(1)
    }//是否有参数
    val conf = new SparkConf().setAppName("User Cusuming Data Statistics").setMaster("spark://ubuntu1:7077")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    sc.addJar("file://F/program/java/spark/jar/user.jar")
    val sqlContext = new SQLContext(sc)
    //this is used to implicity convert an RDD to a DataFrame
    import sqlContext.implicits._
    //Convert user data RDD to a DataFrame and register it as a temp table
    val userFile = sc.textFile(args(0))
    val userSchemaString = "userID,gender,age,registerDate,role,region"
    val userSchemaArray = userSchemaString.split(",")
    val userSchema = StructType(userSchemaArray.map(fieldName =>
      StructField(fieldName, StringType, true)))
    val userRowRDD : RDD[Row] = userFile.map(_.split(" "))
                    .map(u => Row(u(0), u(1), u(2), u(3), u(4), u(5)))
    val userDF = sqlContext.createDataFrame(userRowRDD, userSchema)
    userDF.registerTempTable("user")
    
    val orderFile = sc.textFile(args(1))                
    val orderSchemaString = "orderID,orderDate,productID,price,userID"
    val orderSchemaArray = orderSchemaString.split(",")
    val orderSchema = StructType(orderSchemaArray.map(fieldName =>
      StructField(fieldName, StringType, true)))
    val orderRowRDD : RDD[Row] = orderFile.map(_.split(" "))
                    .map(o => Row(o(0), o(1), o(2), o(3), o(4)))             
    val orderDF = sqlContext.createDataFrame(orderRowRDD, orderSchema)                 
    orderDF.registerTempTable("order")
    userDF.persist(StorageLevel.MEMORY_ONLY_SER)
    orderDF.persist(StorageLevel.MEMORY_ONLY_SER)
    
    //select data from temp table
    val count = orderDF.filter(orderDF("orderDate").contains("2015")).join(userDF,orderDF("userID").equalTo(userDF("userID"))).count()
    println("The number of people who have orders in the year 2015:" + count)
    
//    val countOfOrder_2014 = sqlContext.sql("SELECT * FROM order WHERE orderDate like '2014%'").count()
//    println("Total orders produced in the year 2014:" + countOfOrder_2014)
    
    val countOfOrderForUser = sqlContext.sql("SELECT o.orderID,o.productID,o.price,u.userID FROM order o, user u WHERE u.userID = o.userID and u.userID = '1'").show()
    println("Order produced by user with ID 1 showed")
    
    val orderStatsForUser2 = sqlContext.sql("SELECT max(o.price) as maxPrice, min(o.price) as minPrice, avg(o.price) as avgPrice, u.userID FORM"
	                  + "order o, user u WHERE u.userID = o.userID and u.userID = '10' GROUP BY  u.userID")
	  println("Order statistics results for user with ID 10 : ")
	  orderStatsForUser2.collect()
	                    .map(order => "MaxPrice=" + order.getAs("maxPrice") + "MinPrice=" + order.getAs("minPrice") + "AvgPrice=" + order.getAs("avgPrice"))
	                    .foreach(println)
	                    
  }
}
case class User(userID : String, gender : String, age : Int, registerDate : String, role : String, region : String)
case class Order(orderID : String, orderDate : String, productID : Int, price : Int, userID : String)