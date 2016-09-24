package cn.edu.neu.spark.chapter3

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

case class Person(name: String, age: Int)
object PracticeSQL {
  def main(args : Array[String]) : Unit = {
    if(args.length < 0){
      println("Usage:PeopleDataStatistics Parameter: ")
      System.exit(1)
    }//是否有参数
    val conf = new SparkConf().setAppName("Practice SQL").setMaster("spark://ubuntu1:7077") //配置name和master
    val sc = new SparkContext(conf) // 加载spark上下文，控制整个程序的生命周期
    sc.addJar("file://F:/program/java/spark/jar/sql.jar")
    val sqlContext = new SQLContext(sc) //创建一个sql的上下文，该对象依赖于sparkcontext
//    val hiveContext = new HiveContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
    ///-----------------通过直接加载的方式创建DataFrame-----------------------------------
    println("通过直接加载的方式创建DataFrame")
    val json = sqlContext.read.json("hdfs://ubuntu1:9000/user/zhangph/examples/src/main/resources/people.json")
    json.show()
    val quet = sqlContext.read.load("hdfs://ubuntu1:9000/user/zhangph/examples/src/main/resources/users.parquet")
    quet.show()
    quet.select("name").show()
    quet.select("name", "favorite_color").write.save("hdfs://ubuntu1:9000/user/zhangph/output/namesAndFavColors.parquet")
    
    ///-----------------通过编程方式来创建DataFrame--------------------------------------
    println("通过编程方式来创建DataFrame")
    // Create an RDD
    val people = sc.textFile("hdfs://ubuntu1:9000/user/zhangph/examples/src/main/resources/people.txt")
    // The schema is encoded in a string
    val schemaString = "name age" 
    // Import Row and Import Spark SQL data types
    import org.apache.spark.sql.Row;
    import org.apache.spark.sql.types.{StructType,StructField,StringType};
    
    // Generate the schema based on the string of schema
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    
    // Convert records of the RDD (people) to Rows.
    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))
    
    // Apply the schema to the RDD.
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    
    // Register the DataFrames as a table.
    peopleDataFrame.registerTempTable("people")
    
    // SQL statements can be run by using the sql methods provided by sqlContext.
    val results = sqlContext.sql("SELECT name FROM people")
    
    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by field index or by field name.
    results.map(t => "Name: " + t(0)).collect().foreach(println)
    
    ///-----------------------通过反射机制来创建DataFrame----------------------------------
    println("通过反射机制来创建DataFrame")
    // Define the schema using a case class.
    // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
    // you can use custom classes that implement the Product interface.

    // Create an RDD of Person objects and register it as a table.
    val people1 = sc.textFile("hdfs://ubuntu1:9000/user/zhangph/examples/src/main/resources/people.txt")
                    .map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
    people1.registerTempTable("people1")
    
    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")
    
    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by field index:
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
    
    // or by field name:
    teenagers.map(t => "Name: " + t.getAs[String]("name")).collect().foreach(println)
    
    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagers.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
    
    
    // Map("name" -> "Justin", "age" -> 19)
    /*
        //creating dataframes and dataframe operations
    val df = sqlContext.read.json("hdfs://ubuntu1:9000/user/zhangph/examples/src/main/resources/people.json")
    // Displays the content of the DataFrame to stdout
    df.show()
    println("Show the schema information #1")
    //Print the schema in a tree format
    df.printSchema()
    println("Print the schema in a tree format #2")
    //Select only the "name" column
    df.select("name").show()
    println("Select only the 'name' column #3")
    
    //Select everybody, but increment the age by 1
    df.select(df("name"), df("age") + 1).show()
    println("Select everybody, but increment the age by 1 #4")
    //Select people older than 21
    df.filter(df("age") > 21).show()
    println("Select people older than 21 #5")
    //Count people by age
    df.groupBy("age").count().show()
    println("Count people by age #6")
    
    //Running SQL Queries Programmatically
    //The sql function on a SQLContext enables applications to run SQL queries programmatically and returns the result as a DataFrame.
    //Creating Datasets
//    val ds = Seq(1, 2, 3).toDS()
//    ds.map(_ + 1).collect() // Returns: Array(2, 3, 4)
//    // Encoders are also created for case classes.
//    case class Person(name: String, age: Long)
//    val ds = Seq(Person("Andy", 32)).toDS()
//    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name.
//    val path = "examples/src/main/resources/people.json"
//    val people = sqlContext.read.json(path).as[Person]
    
//    val df1 = sqlContext.sql("SELECT * FROM table")
    // sc is an existing SparkContext.
    
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
     * */
  }
}
