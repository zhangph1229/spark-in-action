package cn.edu.neu.spark.chapter4

import java.io._

object ChangeFile {
  def main(args: Array[String]): Unit = {
    var bfr : BufferedReader = null
    var bfw1 : BufferedWriter = null
    var bfw2 : BufferedWriter = null
    try {
      bfr = new BufferedReader(new FileReader("Wholesale customers data.csv"))
      bfw1 = new BufferedWriter(new FileWriter("train.txt"))
      bfw2 = new BufferedWriter(new FileWriter("test.txt"))
      val regex = ","
      var line = ""
      var count = 0
      while((line = bfr.readLine()) != Nil){
    	  if(line == null) return
        if(count < 300) {
          bfw1.append(line)
          bfw1.append(System.getProperty("line.separator"))
        }else{
          bfw2.append(line)
          bfw2.append(System.getProperty("line.separator"))
        }
        count += 1
        println(count)
      }
      bfw1.flush()
      bfw2.flush()
    } catch {
      case e: Throwable => e.printStackTrace() // TODO: handle error
    } finally{
      try {
        bfw1.close()
        bfw2.close()
        bfr.close()
      } catch {
        case e: IOException => e.printStackTrace() // TODO: handle error
      }
    }
  }
}