
import breeze.math.i
import org.apache.hadoop.io.BooleanWritable.Comparator
import utils.primitives

object Sorting {

  /*def main(args: Array[String]): Unit = {
    //var myarr = new Array[SimpleTuple]( 15 )
    //myarr = A// SimpleTuple ( 1, 3.45 ), SimpleTuple ( 2, 4.65 ), SimpleTuple ( 3, 7.68 ) )

    val r = scala.util.Random
    val myarr = ( for (i <- 1 to 1000000) yield SimpleTuple( i, r.nextInt(1000000000) ) ).toArray
    //println("array da ordinare ")
    //myarr.take(100).map( x=> println(x))

    //val res = SegmentQuickSort(myarr,0,myarr.length)
    println("ordinamento di 1 MLN di istanze")
    time( blockOfCode(myarr) )
    //SegmentQuickSort(myarr, 3, 9)
    (myarr,2)
    println("array ordinato ")
    myarr.take(100).map( x=> println(x))

  }*/

  def time[R](block: => R): R = {

    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    Console.out.println("Elapsed time: " + (t1 - t0)/1000000 + "ms")
    Console.out.println("Elapsed time: " + (t1 - t0)/1000000000 + "sec")

    result
  }

  def blockOfCode(myarr: Array[SimpleTuple]): Unit ={
    var iter= Math.abs(Math.log(myarr.length))
    parMergeSort(myarr,  5);
  }

  def parMergeSort(xs: Array[SimpleTuple], maxDepth: Int) = {

   //array di appoggio
   val ys = new Array[SimpleTuple](xs.length)

   def sort(from: Int, until: Int, depth: Int):Unit = {
     if (depth == maxDepth) {
        SegmentQuickSort(xs, from, until)
      } else {
        val mid = (from + until) / 2

        primitives.parallel(
          sort(mid, until, depth + 1),
          sort(from, mid, depth + 1)
        )

        val flip = (maxDepth - depth) % 2 == 0
        val src = if (flip) ys else xs
        val dst = if (flip) xs else ys
        merge(src, dst,from, mid, until)
      }
   }
    sort(0,xs.length,0 )
  }

  def merge(src: Array[SimpleTuple], dst: Array[SimpleTuple], from: Int, mid: Int, until: Int) = {

    var left = from
    var right = mid
    var i = from

    while (left < mid && right < until) {
      while (left < mid && src(left).value <= src(right).value) {
        dst(i) = src(left)
        i += 1
        left += 1
      }

      while (right < until && src(right).value <= src(left).value) {
        dst(i) = src(right)
        i += 1
        right += 1
      }

    }

    while (left < mid){
      dst(i)=src(left)
      i += 1
      left += 1
    }

    while (right < until) {
      dst(i) = src(right)
      i += 1
      right += 1
    }
  }


  def SegmentQuickSort(xs: Array[SimpleTuple], from: Int, until: Int) = {

     // the quicksort recursive algorithm in the segment [from,until]
     def quickSort(arr: Array[SimpleTuple]): Array[SimpleTuple] = {
       if( arr.length <= 1 ){
         arr
       }else{
         val pivot = arr ( arr.length / 2 )
         Array.concat (
           quickSort ( arr filter (pivot.value > _.value) ),
           arr filter (pivot.value == _.value),
           quickSort ( arr filter (pivot.value < _.value) )
         )
       }
     }

    var ys = new Array[SimpleTuple]( until - from )
    var i = from;
    var j = 0;

    while (i<until) {
      ys(j) = xs(i)
      i = i+1;
      j = j+1;
    }

    ys = quickSort(ys)

    i = from;
    j = 0;
    while (i<until) {
      xs(i) = ys(j)
      i = i+1;
      j = j+1;
    }

   }






}
