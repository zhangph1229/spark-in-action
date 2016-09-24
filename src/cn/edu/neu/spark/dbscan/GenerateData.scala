package cn.edu.neu.spark.dbscan
import java.io._
object GenerateData {
  def main(args: Array[String]): Unit = {
    val file1 = new File("C:/Users/zhangph/Desktop/cluster_result.txt")
    val file2 = new File("cluster_2_res.txt")
    var read : BufferedReader = null
    var writer : FileWriter = null
    try {
      read = new BufferedReader(new FileReader(file1))
      writer = new FileWriter(file2,false)
      var line : String = null
      while((line = read.readLine()) != null || !line.contains(",")){
        val split = line.split(",")
        if(split.length > 1){
        	val v1 = split(2).toDouble/100000.0
        	val v2 = split(3).toDouble/100000.0
        	writer.append(v1+","+v2)
        	writer.append(System.getProperty("line.separator"))
        }
      }
      writer.flush()
    } catch {
      case e: IOException => e.printStackTrace() // TODO: handle error
    }finally{
      read.close()
      writer.close()
    }
  }
}