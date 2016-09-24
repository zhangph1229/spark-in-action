package cn.edu.neu.spark.chapter1.test

import java.io.File
import java.io.FileWriter
import scala.util.Random

object GenerationData {
  def main(args: Array[String]): Unit = {
    println("generation age file to location")
    generationAge()
    println("generation people information ")
    generationPeople()
  }

  def generationAge(): Unit = {
    val path = "ageData.txt"
    val writer = new FileWriter(new File(path))
    val rand = new Random()
    for (i <- 1 to 10000) {
      writer.write(i + " " + rand.nextInt(110))
      writer.write(System.getProperty("line.separator"))
    }
    writer.flush()
    writer.close()
  }

  def generationPeople(): Unit = {
    val path = "peopleData.txt"
    val writer = new FileWriter(new File(path))
    val rand = new Random()
    for (i <- 1 to 10000) {
      var height = rand.nextInt(220)
      if (height < 50) height += 50
      writer.write(i + " " + getSex() + " " + height)
      writer.write(System.getProperty("line.separator"))
    }
    writer.flush()
    writer.close()
  }

  def getSex(): String = {
    val rand = new Random()
    val num = rand.nextInt(100)
    if (num % 2 == 0) {
      "F"
    } else {
      "M"
    }
  }

}