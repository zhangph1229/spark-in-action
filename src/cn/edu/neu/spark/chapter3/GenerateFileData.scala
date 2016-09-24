package cn.edu.neu.spark.chapter3

import java.io.FileWriter
import scala.util.Random

object GenerateFileData {
  private val ROLE_ARRAY = Array[String]("ROLE001", "ROLE002", "ROLE003", "ROLE004", "ROLE005")
  private val REGION_ARRAY = Array[String]("REG001", "REG002", "REG003", "REG004", "REG005")
  private val PRODUCT_ARRYA = Array[Int](1,2,3,4,5,6,7,8,9,10)
  private val MAX_USER_AGE = 70
  private val MAX_PRICE = 2000
  private val MIN_PRICE = 10
  private val MAX_RECORDS = 100000
  private val MAX_DEAL = 1000000
  private val FILE_USER = "userData.txt"
  private val FILE_DEAL = "dealData.txt"
  private val rand = new Random()
  def generateUserInfo(file : String, num : Int) : Unit = {
    var writer : FileWriter = null
    try{
    	writer = new FileWriter(file)
      for(i <- 1 to num){
        val gender = getSex 
        var age = rand.nextInt(MAX_USER_AGE) 
        if(age < 10) age += 10
        val registerDate = getDate
        val role = ROLE_ARRAY(rand.nextInt(ROLE_ARRAY.length))
        val region = REGION_ARRAY(rand.nextInt(REGION_ARRAY.length))
        writer.append(i + " " + gender + " " + age + " " + registerDate + " " + role + " " + region)
        writer.append(System.getProperty("line.separator"))
      }
      writer.flush()
    }catch{
      case e : Exception => println("Exception : " + e)
    }finally{
      if(writer != null) writer.close()
    }
  }
  def getSex  : String = {
    if((rand.nextInt() + 1) % 2 == 0) "M"
    else "F"
  }
  def getDate : String = {
    var year = 2000 + rand.nextInt(17)
    var month = rand.nextInt(12) + 1
    var day = rand.nextInt(28) + 1
    year + "-" + month + "-" + day
  }
  
  def generateDealInfo(file : String , num : Int) : Unit = {
    var writer : FileWriter = null
    try {
      writer = new FileWriter(file)
      for(i <- 1 to num){
    	  val dealDate = getDate
        val productID = PRODUCT_ARRYA(rand.nextInt(PRODUCT_ARRYA.length))
        var productPrice = rand.nextInt(MAX_PRICE)
        if(productPrice < MIN_PRICE) productPrice += MIN_PRICE
        val userID = rand.nextInt(MAX_RECORDS) + 1
        writer.append(i + " " + dealDate + " " + productID + " " + productPrice + " " + userID)
        writer.append(System.getProperty("line.separator"))
      }
      writer.flush()
    } catch {
      case e: Throwable => e.printStackTrace() // TODO: handle error
    }finally{
      if(writer != null) writer.close()
    }
  }
  def main(args: Array[String]): Unit = {
    generateUserInfo(FILE_USER, MAX_RECORDS)
    println("user info was generated, please check in " + FILE_USER)
    
    generateDealInfo(FILE_DEAL, MAX_DEAL)
    println("deal info generated successfully.")
  }
}