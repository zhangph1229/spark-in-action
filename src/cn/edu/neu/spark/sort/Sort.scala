package cn.edu.neu.spark.sort

object Sort {
    case class BubbleSort[T<%Ordered[T]](val array:Array[T]){  
      private[this] var lastIndex = array.length-1  
      private[this] var notFinish = false  
      private[this] def sortFromIndex(index:Int):Unit= index match {  
        case last if last==lastIndex => if(lastIndex>1 && notFinish){  
          lastIndex=lastIndex-1  
          notFinish=false  
          sortFromIndex(0)  
        }  
        case otherIndex => {  
          if(array(otherIndex)>array(otherIndex+1)) swap(otherIndex, otherIndex+1)  
          sortFromIndex(otherIndex+1)  
        }  
      }  
      private[this] def swap(index:Int, nextIndex:Int) = {  
        val temp = array(nextIndex)  
        array(nextIndex)=array(index)  
        array(index)=temp  
      }  
      def bubbleSort=sortFromIndex(0)  
    }  
    object BubbleSort{  
      implicit def arrToBubbleSort[T <% Ordered[T]](arr:Array[T]):BubbleSort[T]=BubbleSort(arr)  
    }  
    def insertionSort[T <% Ordered[T]](arr:Array[T])(f:(T,T)=>Boolean)={  
      for((index, item)<-(0 until arr.length) zip arr){  
        if(index!=0 && f(item,arr(index-1))){  
          var insertIndex=index-1  
          while(insertIndex>=0 && f(item,arr(insertIndex))){  
              arr(insertIndex+1)=arr(insertIndex)  
              insertIndex-=1  
          }  
          arr(insertIndex + 1)=item  
        }  
      }  
    } 
    
    def main(args: Array[String]): Unit = {
      val array = Array(2,1,4,6,2,9,66,54,78,91,20,47,0,4,-23)  
      insertionSort(array){_>_}  
      array foreach println  
    }
}