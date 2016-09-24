package cn.edu.neu.spark.practice

import scala.util.Random

object KMeans {
  val POINTS = List(Point(0, 0),Point(1,0),Point(0,1),Point(1,1),Point(1,2),Point(2,1),
              Point(2,4),Point(3,4),Point(4,4),Point(3,5),Point(4,5),
              Point(4,1),Point(4,2),Point(5,0),Point(5,1),Point(5,2),Point(6,1))
//  println(POINTS.length)
  def main(args: Array[String]): Unit = {
    val res = k_means(3)
    res.foreach { list => list.foreach { point => println(point.x,point.y) } }
  }
  def k_means(k : Int) : Array[List[Point]] = {
    val rand = new Random()
    var first = rand.nextInt(POINTS.length - 2)
    var p1 = POINTS(first)
    var second = rand.nextInt(POINTS.length - 2)
    var p2 : Point = POINTS(second); 
    if(first == second )
      p2 = POINTS(second + 1)
    var third =   rand.nextInt(POINTS.length)
    var p3 = POINTS(third);
    if(third == first || third == second) 
      p3 = POINTS(third + 2)
    var cu1  = p1 :: Nil
    var cu2  = p2 :: Nil
    var cu3  = p3 :: Nil
    var tp1 : Point = null 
    var tp2 : Point= null 
    var tp3 : Point= null
    while(!(p1.equals(tp1) && p2.equals(tp2) && p3.equals(tp3))){
      tp1 = p1;tp2 = p2;tp3 = p3
      for(i <- 0 to POINTS.length - 1){
        val point = POINTS(i)
        var d1 = point.getDist(p1)
        var d2 = point.getDist(p2)
        var d3 = point.getDist(p3)
        println(d1 + " " + d2 + " " + d3)
        if(d1 < d2 && d1 < d3){ 
          cu1 :: point :: Nil
          cu2.dropWhile(x => cu2.contains(point))
          cu3.dropWhile(x => cu3.contains(point))
        } else if(d2 < d1 && d2 < d3) {
          cu2 :: point :: Nil
          cu1.dropWhile(x => cu1.contains(point))
          cu3.dropWhile(x => cu3.contains(point))
        }else if(d3 < d1 && d3 < d2) {
          cu3 :: point :: Nil
          cu1.dropWhile(x => cu1.contains(point))
          cu2.dropWhile(x => cu2.contains(point))
        }
      }
      p1 = getCenter(cu1)
      p2 = getCenter(cu2)
      p3 = getCenter(cu3)
    }
    Array(cu1,cu2,cu3)
  }
  
  def getCenter(list : List[Point]) : Point = {
    var flag : Int = 0;
    var temp = 10000.0
    for(i <- 0 to list.length - 1){
      val point1 = list(i)
      var dist = 0.0
      for(j <- 0 to list.length - 1){
        val point2 = list(j)
        dist += getDist(point1,point2)
      }
      if(dist < temp){
        temp = dist
        flag = i
        println(flag)
      }
    }
    POINTS(flag)
  }
   def getDist(p1:Point, p2:Point) : Double = {
      Math.pow(Math.pow(p1.x-p2.x,2) + Math.pow(p1.y-p2.y,2),0.5)
  }
}
case class Point(x : Double , y : Double){
  def getDist(p : Point) : Double = {
    Math.pow(Math.pow(x - p.x,2)+ Math.pow(y - p.y,2), 0.5)
  }

}







