package cn.edu.neu.spark.practice

import scala.util.Random
import java.io.FileWriter
import java.io.File

object GenerateData {
  //生成年龄数据，用于分析平均年龄
  def FILE_NAME : String = "ageData.txt"
  def generateAgeInfo(nums : Int) : Unit = {
    val rand = new Random()
    val writer = new FileWriter(FILE_NAME, false)
    System.out.println(System.getProperty("user.dir") + System.getProperty("line.separator"))
    for(i <- 0 to nums){
      writer.append(i + " "+ rand.nextInt(110))
      writer.append(System.getProperty("line.separator"))
    }
    writer.flush()
    writer.close()
    println("generated data in " + FILE_NAME)
  }
  
  def FILE_NAME_SEX = "sexheightData.txt"
  def sexHeight(nums : Int):Unit = {
    val rand = new Random()
    val writer = new FileWriter(new File(FILE_NAME_SEX), false)
    for(i <- 0 to nums){
      var height = rand.nextInt(200) + rand.nextFloat()
      if(height > 220) height = rand.nextInt(200)
      if(height < 50) height += 50
      writer.append(i + " " + getSex + " " + height)
      writer.append(System.getProperty("line.separator"))
    }
    writer.flush()
    writer.close()
    println("generated data in " + FILE_NAME_SEX)
  }
  def getSex : String = {
    val rand = new Random()
    if(rand.nextInt(10) % 2 == 0){
      "M"
    }else{
      "F"
    }
  }
  def getDate : String = {
    val rand = new Random()
    val year = 2000 + rand.nextInt(17)
    val month = rand.nextInt(12) + 1
    val day = rand.nextInt(28) + 1
    year + "-" + month + "-" + day
  }
  private val ROLE_ARRAY = Array[String]("R1","R2","R3","R4","R5")
  private val REGION_ARRAY = Array[String]("L1","L2","L3","L4","L5","L6","L7","L8")
  private val MAX_USER = 10000
  private val MAX_AGE = 70
  def generateUserInfo(file : String, num : Int) : Unit = {
    var writer:FileWriter = null
    try {
      writer = new FileWriter(file)
      val rand = new Random()
      for(i <- 1 to num){
        val sex = getSex
        var age = rand.nextInt(MAX_AGE)
        if(age < 10) age += 10
        val date = getDate
        val role = ROLE_ARRAY(rand.nextInt(ROLE_ARRAY.length))
        val region = REGION_ARRAY(rand.nextInt(REGION_ARRAY.length))
        writer.append(i + " " + sex + " " + age + " " + date + " " + role + " " + region)
        writer.append(System.getProperty("line.separator"))
      }
      writer.flush()
      println("generateUserInfo")
    } catch {
      case e: Throwable => e.printStackTrace() // TODO: handle error
    }finally{
      if(writer != null ) writer.close()
    }
  }
  private val MAX_ORDER = 100000
  private val MAX_PRICE = 10000
  private val MIN_PRICE = 5000
  def generateOrderInfo(file : String, num : Int) {
    val rand = new Random()
    var writer : FileWriter = null
    try{
      writer = new FileWriter(file)
      for(i <- 1 to num){
        val date = getDate
        val productType = rand.nextInt(10)
        var price = rand.nextInt(MAX_PRICE) + 1 + rand.nextFloat()
        if(price > MAX_PRICE) price = MAX_PRICE
        else if(price < MIN_PRICE) price = MIN_PRICE
        val userID = rand.nextInt(MAX_USER)
        writer.append(i + " " + date + " " + productType + " " + price + " " + userID)
        writer.append(System.getProperty("line.separator"))
      }
      writer.flush()
      println("generateOrderInfo")
    }catch{
      case e : Exception => println(e)
    }finally{
      if(writer != null ) writer.close()
    }
  }
  
  def main(args : Array[String]) : Unit = {
//    generateAgeInfo(100000)
//    sexHeight(100000)
    generateUserInfo("userData.txt", MAX_USER)
    generateOrderInfo("orderData.txt", MAX_ORDER)
  }
  
}