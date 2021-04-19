import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.{GenSeq, mutable}
import scala.math.log

class Sorting2(xs: Array[SimpleTuple], maxDepth: Int ) extends Serializable {

  def parMergeSort(): Unit = {
    val ys = new Array[SimpleTuple](xs.length)

    def sort(from: Int, until: Int, depth: Int): Unit = {
      if (depth == maxDepth) {
        quickS(xs, from, until)
      } else {
        val mid = (from + until) / 2
        parallel(sort(mid, until, depth + 1),
          sort(from, mid, depth + 1))
        val flip = (maxDepth - depth) % 2 == 0
        val src = if (flip) ys else xs
        val dst = if (flip) xs else ys
        merge(src, dst, from, mid, until)
      }
    }

  }

  private def merge(xs: Array[SimpleTuple], ys: Array[SimpleTuple]): Array[SimpleTuple] =
    xs match {
      case Nil => ys
      case x :: xs1 =>
        ys match {
          case Nil => xs
          case y :: ys1 =>
            if( x.value > y.value ) x :: merge ( xs1, ys )
            else y :: merge ( xs, ys1 )
        }
    }

  private def quickS(xs: Array[SimpleTuple], from: Int, until: Int) = {
    // create an array of random 10000 random ints
    val r = scala.util.Random
    val randomArray = (for (i <- 1 to 1000) yield r.nextInt(100000)).toArray

    // do the sorting
    val sortedArray = quickSort(randomArray)

    // print the ordered array
    sortedArray.foreach(println)

    // the quicksort recursive algorithm
    def quickSort(xs: Array[SimpleTuple]): Array[SimpleTuple] = {
      if (xs.length <= 1) xs
      else {
        val pivot = xs(xs.length / 2)
        Array.concat(
          quickSort(xs filter (pivot > _)),
          xs filter (pivot == _),
          quickSort(xs filter (pivot < _)))
      }
    }
  }

  /*
  def msort(xs: List[SimpleTuple]): List[SimpleTuple] = {
    val n = xs.length / 2
    if( n == 0 ) xs else {
      def merge(xs: List[SimpleTuple], ys: List[SimpleTuple]): List[SimpleTuple] =
        xs match {
          case Nil => ys
          case x :: xs1 =>
            ys match {
              case Nil => xs
              case y :: ys1 =>
                if( x.value > y.value ) x :: merge ( xs1, ys )
                else y :: merge ( xs, ys1 )
            }
        }

      val (fst, snd) = xs splitAt n
      merge ( msort ( fst ), msort ( snd ) )
    }
  }

  def isort(xs: List[Int]): List[Int] = xs match {
    case List () => List ()
    case y :: ys => insert ( y, isort ( ys ) )
  }

  def insert(x: Int, xs: List[Int]): List[Int] = xs match {
    case List () => List ( x )
    case y :: ys => if( x < y ) x :: xs else y :: insert ( x, ys )
  }
*/

}