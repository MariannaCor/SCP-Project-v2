import utils.primitives
class Sorting {

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


  def main(args: Array[String]): Unit = {
    /* Test Sorting */
    val list_one = List [Double]( 8, 7, 3, 5, 9, 6, 1, 4, 2, 10 )
    println ( "ordered lists are " )
    //msort(list_one) map( i => print( i+", ") )

    println ()
    val list_two = List [Int]( 8, 7, 3, 5, 9, 6, 1, 4, 2, 10 )
    isort ( list_two ) map (i => print ( i + ", " ))
    println ()

    val list_three = List [Int]( 8, 7, 3, 5, 9, 6, 1, 4, 2, 10 )
    list_three.sorted map (i => print ( i + ", " ))

    System.exit ( 0 );

  }

  def quickS(xs: Array[Int], from: Int, until: Int) = ???

  def parMergeSort(xs: Array[Int], maxDepth: Int): Unit = {
    val ys = new Array[Int](xs.length)

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



}
