package cn.edu.neu.spark.practice

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

case class UserSchema(userID: String, sex: String, age: Int, date: String, role: String, region: String)
case class OrderSchema(orderID: String, date: String, productType: String, price: Double, userID: String)
object ConsumingDataStatistics {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("User Order Data Statistics").setMaster("spark://ubuntu1:7077")
    val sc = new SparkContext(conf)
    sc.addJar("file://F:/program/java/spark/jar/consume.jar")
    val sqlContext = new SQLContext(sc)
    val userData = sc.textFile("hdfs://ubuntu1:9000/user/zhangph/input/userData.txt", 3)
    val orderData = sc.textFile("hdfs://ubuntu1:9000/user/zhangph/input/orderData.txt", 5)
    import sqlContext.implicits._
    //----使用构建的方式-----------
    import org.apache.spark.sql.types.{ StructType, StructField, StringType }
    import org.apache.spark.sql.Row
    val schemaString_user = "userID,sex,age,date,role,region"
    val schema_user = StructType(schemaString_user.split(",").map(line =>
      StructField(line, StringType, true)))
    val row_user = userData.map(_.split(" ")).map(r => Row(r(0), r(1), r(2).toInt, r(3), r(4), r(5)))
    val userDF = sqlContext.createDataFrame(row_user, schema_user)
    userDF.registerTempTable("user")

    val schemaString_order = "orderID,date,productType,price,userID"
    val schema_order = StructType(schemaString_order.split(",")
      .map(line => StructField(line, StringType, true)))
    val row_order = orderData.map(_.split(" ")).map(r => Row(r(0), r(1), r(2), r(3).toDouble, r(4)))
    val orderDF = sqlContext.createDataFrame(row_order, schema_order)
    orderDF.registerTempTable("orders")

    //-----使用反射的方式---------
    val userDataFrame = userData.map(_.split(" "))
      .map(u => UserSchema(u(0), u(1), u(2).toInt, u(3), u(4), u(5))).toDF()
    userDataFrame.registerTempTable("user1")

    val orderDataFrame = orderData.map(_.split(" "))
      .map(o => OrderSchema(o(0), o(1), o(2), o(3).toDouble, o(4))).toDF()
    orderDataFrame.registerTempTable("order1")
    println("----------------表结构构造完成----------------")
 
    val user_order_2015 = orderDataFrame.filter(orderDataFrame("date").contains("2015"))
      .join(userDataFrame, orderDataFrame("userID").equalTo(userDataFrame("userID")))
    user_order_2015.show(10)
    println("user_order_2015.count = " + user_order_2015.count())
    println("<Display #1>")
    
    val order_2014 = orderDataFrame.filter(orderDataFrame("date").contains("2014"))
    order_2014.show(10)
    println("order_2014.count=" + order_2014.count())
    val order_2014_1 = sqlContext.sql("SELECT * FROM order1 WHERE date like '2014%'").count()
    println("order_2014_1.count = " + order_2014_1)    
    println("<Display #2>")
    
    val user_1_order = orderDataFrame.filter(orderDataFrame("userID").equalTo("1"))
       .join(userDataFrame,orderDataFrame("userID").equalTo(userDataFrame("userID")));
    user_1_order.show()
    println("user_1_order = " + user_1_order.count()) 
    val user_2_order = sqlContext.sql("SELECT o.orderID, o.price, u.userID FROM order1 o,user1 u where u.userID = '2' and u.userID = o.userID").show()
    println("Orders produced by user with ID 1 showed.")
    println("<Display #3>")
 
    val order_user9 = orderDataFrame.filter(orderDataFrame("userID").equalTo("9"))
      .join(userDataFrame,orderDataFrame("userID").equalTo(userDataFrame("userID"))).cache()
      
    val max = order_user9.agg("price" -> "max").show()
    val min = order_user9.agg("price" -> "min").show()
    val avg = order_user9.agg("price" -> "avg").show()
//    val min_user9_price = sqlContext.sql("SELECT min(o.price) as minPrice, avg(o.price) as avgPrice FROM order1 o, user1 u WHERE o.userID = u.userID and userID = '9' GROUP BY u.userID")
//    println("min_user9_price = " + min_user9_price.collect.map(o => "MinPrice = " + o.getAs("minPrice") + "AvgPrice = " + o.getAs("avgPrice")))
    println("<Display #4>")
  }
}