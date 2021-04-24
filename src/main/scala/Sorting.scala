
import breeze.math.i
import org.apache.hadoop.io.BooleanWritable.Comparator
import utils.primitives

object Sorting {

  /*
  def main(args: Array[String]): Unit = {
    //var myarr = new Array[SimpleTuple]( 15 )
    //myarr = A// SimpleTuple ( 1, 3.45 ), SimpleTuple ( 2, 4.65 ), SimpleTuple ( 3, 7.68 ) )

    val r = scala.util.Random
    val myarr = ( for (i <- 1 to 1000000) yield SimpleTuple( i, r.nextInt(10000000) ) ).toArray
    println( "L'array è ordinato ? "+checkSoundness(myarr))
    println("ordinamento di 1 MLN di istanze")
    time( blockOfCode(myarr) )
    //SegmentQuickSort(myarr, 0, myarr.length)
    println( "L'array è ordinato ? "+checkSoundness(myarr))
    
  }

  def time[R](block: => R): R = {

    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    Console.out.println("Elapsed time: " + (t1 - t0)/1000000 + "ms")
    Console.out.println("Elapsed time: " + (t1 - t0)/1000000000 + "sec")

    result
  }
*/

  /* it returns true if the array is ordered from higher to lower value */
  def checkSoundness(myarr: Array[(Int,Double)]): Boolean = {
    var i = 0 ;
    while (i < myarr.length - 2 ){
      if( myarr(i)._2 < myarr(i+1)._2 ) {  return false  }
      i = i+1;
    }
    true
  }


  def blockOfCode(myarr: Array[(Int,Double)]): Unit ={
    parMergeSort(myarr,  4);
  }

  def parMergeSort(xs: Array[(Int,Double)], maxDepth: Int) = {

    if (maxDepth % 2 == 1) throw new Exception("max depth param must be an even number")
   //array di appoggio
   val ys = new Array[(Int,Double)](xs.length)

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
/*
*  def merge(src: Array[SimpleTuple], dst: Array[SimpleTuple], from: Int, mid: Int, until: Int) = {

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
* */


  def merge(src: Array[(Int,Double)], dst: Array[(Int,Double)], from: Int, mid: Int, until: Int) = {

    var left = from
    var right = mid
    var i = from

    while (left < mid && right < until) {
      while (left < mid && src(left)._2 >= src(right)._2) {
        dst(i) = src(left)
        i += 1
        left += 1
      }

      while (right < until && src(right)._2 >= src(left)._2) {
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


  def SegmentQuickSort(xs: Array[(Int,Double)], from: Int, until: Int) = {

     // the quicksort recursive algorithm in the segment [from,until]
     def quickSort(arr: Array[(Int,Double)]): Array[(Int,Double)] = {
       if( arr.length <= 1 ){
         arr
       }else{
         val pivot = arr ( arr.length / 2 )
         Array.concat (
           quickSort ( arr filter (pivot._2 < _._2) ),
           arr filter (pivot._2 == _._2),
           quickSort ( arr filter (pivot._2 > _._2) )
         )
       }
     }

    var ys = new Array[(Int,Double)]( until - from )
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


/*

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
* */



}
