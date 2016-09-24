package cn.edu.neu.spark.scala

object ExampleValues {
  val value : Int = 5
  def fibs : Stream[Int] = 0 #:: 1 #:: fibs.zip(fibs.tail).map(item => item._1 + item._2) 
  
  //quick sort
  def qsort(list : List[Int]) : List[Int] = list match{
    case Nil => Nil
    case pivot :: tail => {
      val (smaller , higher) = tail.partition(_ < pivot) // { x => x < pivot }
      qsort(smaller) ::: pivot :: qsort(higher)
    }
  }
  
  //factorial
  def fact(n : Int) : Int = n match{
    case 0 => 1
    case n => n * fact(n - 1)
  }
  def fact1(n : Int) = (1 to n).reduce(_*_)
  
  //Expr
  trait Expr
  case class Number(n : Int) extends Expr
  case class Sum(expr1 : Expr, expr2 : Expr) extends Expr
  def eval(e : Expr) : Int = e match{
    case n : Number => n.n
    case Sum(expr1, expr2) => eval(expr1) + eval(expr2)
  }
  
  //Tally Votes
  val votes = List(("Scala", 10), ("Java", 20),("Pyton", 3),("Java", 20),("Scala", 2))
  val votesMerage = votes.groupBy(item => item._1)
  def main(args: Array[String]): Unit = {
//    fibs.take(10).foreach(println)
//    val list = List(3,5,1,2)
//    qsort(list).foreach { x => println(x) }
    println(fact1(10))
    println(eval(Sum(Number(1),Sum(Number(2),Sum(Number(4),Number(3))))))
  }
 val increase : PartialFunction[Any, Any] = {
 	case x : Int => x + 1
 }
 increase(1)
 val increase1 : PartialFunction[Any, Any] = increase.orElse{
 	case x : Double => x + 2
 }
 
		 def mywhile(condition : => Boolean)(body : => Unit) : Unit =
		   	if(condition) {body;mywhile(condition)(body)	}
		 var n = 5
		 mywhile(n > 0){
		 	n = n - 1
		 	println(n)
		 }

}