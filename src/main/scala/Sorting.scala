class Sorting {

  def main(args: Array[String]): Unit = {
    var myarr = new Array[SimpleTuple]( 3 )
    myarr = Array ( SimpleTuple ( 1, 3.45 ), SimpleTuple ( 2, 4.65 ), SimpleTuple ( 3, 7.68 ) )

    parMergeSort(myarr, 3);
  }


  def parMergeSort(xs: Array[SimpleTuple], maxDepth: Int): Unit = {
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

  private def merge(xs: List[SimpleTuple], ys: List[SimpleTuple]): Array[SimpleTuple] =
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

    // do the sorting
    val sortedArray = quickSort(xs)

    // print the ordered array
    sortedArray.foreach(println)

    // the quicksort recursive algorithm
    def quickSort(xs: Array[SimpleTuple]): Array[SimpleTuple] = {
      if (xs.length <= 1) xs
      else {
        val pivot = xs(xs.length / 2)
        Array.concat(
          quickSort(xs filter (pivot > _.value)),
          xs filter (pivot == _.value),
          quickSort(xs filter (pivot < _.value)))
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