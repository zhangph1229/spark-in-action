package cn.edu.neu.spark.chapter1

import java.io._
import scala.util.Random

object GenerateData {
  def main(args: Array[String]): Unit = {
    generateAge();
    generatePeople();
  }

  def generateAge() {
    val path = "ageData.txt"
    val writer = new FileWriter(new File(path))
    val rand = new Random()
    for (i <- 1 to 10000) {
      writer.write(i + " " + rand.nextInt(100))
      writer.write(System.getProperty("line.separator"))
    }
    writer.flush()
    writer.close()
  }

  def generatePeople() = {
    val path = "peopleData.txt"
    val writer = new FileWriter(new File(path))
    val rand = new Random()
    for (i <- 1 to 10000) {
      var height = rand.nextInt(220)
      var gender = getRandomGender()
      if (height < 50) height += 50
      writer.write(i + " " + gender + " " + height)
      writer.write(System.getProperty("line.separator"))
    }
    writer.flush()
    writer.close()
  }

  def getRandomGender(): String = {
    val rand = new Random()
    val num = rand.nextInt(10)
    if (num % 2 == 0) {
      "M"
    } else "F"
  }
}