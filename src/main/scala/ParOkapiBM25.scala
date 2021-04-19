import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.{GenSeq, mutable}
import scala.math.log

class ParOkapiBM25(dataFrame: DataFrame, q: GenSeq[String], dfSize: Long ) extends Serializable {

  /*
  |--------------------------------------------------------------------------
  | OkapiBM25 CALCULATION
  |--------------------------------------------------------------------------

  // input: query (termini) e documento (termini)
  // idf del termine (preso da colonna IDF) ,
  */

  private def calculateOkapiBM25(avgDocLen: Double, tfOfWord: Int, idfOfWord: Double, lengthOfWord: Int): Double = {

    lazy val k1 = 1.5 //va da 1.2 a 2.0
    lazy val b = 0.75
    val num = tfOfWord * (k1 + 1)

    val x = b * (lengthOfWord / avgDocLen)
    val y = ((1 - b) + (x))
    val den = tfOfWord + (k1 * y)
    lazy val result = idfOfWord + (num / den)
    result
  }





def calcAvgDocs(pagesLenght: DataFrame): Double = {
 pagesLenght.createOrReplaceTempView("average_tab")
 pagesLenght.sqlContext.sql("select avg(doc_length) from average_tab").head().getDouble(0)
}


def getBM25() = {

  var templist = mutable.MutableList[(String, Int)]()

  var data_docs = for {
    row <- dataFrame.collect().par
    id = row.get( 0 ).toString.toLong
    wordOfDocs = row.get( 1 ).asInstanceOf[mutable.WrappedArray[String]].par

    y = wordOfDocs.flatMap( word => {
      val x: GenSeq[(Int, String, Int)] = q.map( keyword => {
        val res: (Int, String, Int) = (id.toInt, keyword, (if ( keyword == word ) 1 else 0))
        res
      } )
      x
    } )

  } yield {
    /*problema di scrittura e lettura quando non è sequenziale avviene perchè piu thread provano a scrivere su mylist2 concorrentemente * */
    //synchronized{
    //mylist2 = mylist2.++( y )
    q.map( keyword => {
      if( wordOfDocs.indexOf ( keyword ) != -1 ) {
        templist += ((keyword, 1))
      }
    })

    (id, wordOfDocs.length, y) // }
  }


  lazy val df_ofWords: Map[String, Int] = templist.groupBy( _._1 ).mapValues( seq => seq.map( _._2 ).reduce( _ + _ ) ) //tempList groupBy(word => word) mapValues(_.size) //
  lazy val idf_OfWords = df_ofWords.map( tuple => (tuple._1, calcIDFFunc( tuple._2, dfSize )) )
  lazy val tempTF = data_docs.map( _._3 ).reduce( (a1, a2) => a1.++( a2 ) )
  lazy val tf_ofDocs = tempTF.groupBy( x=> (x._1,x._2) ).mapValues( seq => seq.map( _._3 ).reduce( _ + _ ) )
  lazy val docLengths = data_docs.map( x => (x._1, x._2) )

  lazy val totLen = docLengths.reduce( (x, y) => (x._1, x._2 + y._2) )._2
  lazy val avg: Double = totLen.toDouble / docLengths.length.toDouble

  val scores = docLengths.map( (elem) =>
    synchronized {
        val id = elem._1.toInt
        val len = elem._2
        val score = q.map( keyword =>{
          if (idf_OfWords.contains(keyword)){
            val key = (id, keyword)
            val idf: Double = idf_OfWords( keyword )
            val tf: Int = tf_ofDocs( key )
            calculateOkapiBM25( avg, tf, idf, len )
          }else
            0;
        }).reduce( _ + _ )
        SimpleTuple( id, score )
      }).toList

  scores

}

  //return: calcolo dei tf di q su tutti i documenti
  def calcTF(df: DataFrame, word: String): DataFrame = {
    df.groupBy("id").agg(count(when(df("page_tokens") === word, 1)).as(s"cnt_$word"))
  }

  //return: calcolo tutte lunghezze dei documenti
  //input: dataframe processato
  def calcPageLenght(): DataFrame = {
    val lengthUdf = udf { (page_words: mutable.WrappedArray[String]) => page_words.length }
    dataFrame.withColumn("doc_length", lengthUdf(dataFrame("words_clean")))
  }

  //return: calcolo df di tutte le parole nella query (numero di documenti contenenti la parola nella query, per ogni parola)
  def calcDF(df: DataFrame): DataFrame = {
    df.groupBy("page_tokens").agg(countDistinct("id") as "df") //.filter(flattenedDataFrame("page_tokens") === q)
  }

  def calcIDF(dfDataFrame: DataFrame): DataFrame = {
    val calcIDFUdf = udf { (df: Double) => calcIDFFunc(df, dfSize) }
    dfDataFrame.withColumn("idf", calcIDFUdf(col("df")))
  }

  private def calcIDFFunc(df: Double, size: Long): Double = {
    log((size.toDouble + 1) / (df + 1))
  }

}


//var tf_ofWords = mylist2.groupBy(x=> (x._1, x._2) ).mapValues(seq => seq.map(  _._3  ).reduce(_+_))
// println("the list =" + thelist)
/*
   val df_OfWords = mylist.groupBy(_._1).mapValues(seq => seq.map(_._2).reduce(_ + _))
   val idf_OfWords = df_OfWords.map( tuple => {
     (tuple._1, calcIDFFunc(tuple._2, dfSize))
   })

   val avgLength = docLengths.sum.toDouble/docLengths.length.toDouble
*/
 // c.foreach( arrbuff =>  arrbuff.map( x => println(x) )
 //c.map(  println(_)  )
 //(keywordQuery, num di doc in qui c'è quella query )
/*
     values = q.map(word => {
       calcTF(flattenedDataFrame, word).head()
     }).reduce(_+_)
*/

/* VERSIONE LENTA
var scores = for {
row <- dataFrame.collect().par
id: Long = row.get( 0 ).toString.toLong;
value = q.map( word => {
 //println("Doc : " + id + " evaluating the keyword " + word)
 val tf = calcTF(flattenedDataFrame, word)
 tf.createOrReplaceGlobalTempView(s"count_$word")
 //println( "row.get(0) == " + id +" of word "+word)

 var tfOfWord = (tf.sqlContext.sql(s" select cnt_$word from global_temp.count_$word where id == ${id}"))
   .head().getLong(0)
 println(id, word,tfOfWord)
})

}yield{ 0 }
*/
/*
value = q.map( word => {
  //println("Doc : "+id +" evaluating the keyword "+word)
  val tf = calcTF( flattenedDataFrame, word )
  tf.createOrReplaceGlobalTempView( s"count_$word" )
  //println( "row.get(0) == " + id +" of word "+word)
  var tfOfWord  = (tf.sqlContext.sql( s" select cnt_$word from global_temp.count_$word where id == ${id}" ))
    .head().getLong(0)

  var idfOfWordDataRow = (idfDataFrame.select("idf").filter(idfDataFrame("page_tokens") === word))
  var idfOfWord: Double =  0.0
  if(! idfOfWordDataRow.isEmpty)
    idfOfWord = idfOfWordDataRow.head().getDouble( 0 )

  var lengthOfWord = (pagesLenght.sqlContext.sql( s" select doc_length from global_temp.length where id == ${id}" ))
    .head().getInt(0)

  calculateOkapiBM25( avg, q.length ,tfOfWord, idfOfWord, lengthOfWord)

} ).reduce(_+_)*/


/* DEPRECATED
val ret: Array[SimpleTuple] = dataFrame.map( row => {
 println( "secondo flattered" )
 flattenedDataFrame.show( true )
 var id: Long = row.get( 0 ).toString.toLong
 var value: Double =
   q.map( word => {
       println( "terzo flattered" )
       flattenedDataFrame.show( true )
       val tf = calcTF( flattenedDataFrame, word )
       tf.createGlobalTempView( "word_count" )
       println( "row.get(0) == " + id )
       var tfOfWord = (sparkSession.sql( s" select cnt_$word from global_temp.word_count where id == ${id}" ))
         .head().getDouble( 0 )
       var idfOfWord = sparkSession.sql( s" select cnt_$word from global_temp.word_count where id == ${id}" )
         .head().getDouble( 0 )
       var lengthOfWord = sparkSession.sql( s" select cnt_$word from global_temp.word_count where id == ${id}" )
         .head().getDouble( 0 )
       (doFormula( tfOfWord, idfOfWord, lengthOfWord ))

     } ).reduce( _ + _ )

 SimpleTuple( id, value )

} )( Encoders.product[SimpleTuple] ).collect()

ret
//pagesLenght
*/
//scores.foreach(println(_))
// scores.toArray
//}

/*
  println("num :== tfOfWord * ( k1+ 1 ) == "+num)
  println("num :== "+tfOfWord+" * ("+k1+"+ 1 ) == "+num)
  println(" x := b * (lengthOfWord/avgDocLen) == > " + x)
  println(" x := "+b+" * ("+lengthOfWord+"/"+avgDocLen+") == > "+x)
  println(" y := ( ( 1-b ) +  ( x  ) ) == > " + y)
  println(" y := ( ( 1-"+b+" ) +  ( "+x+"  ) ) == > " + y)
  println(" den :=fOfWord + ( k1 *  y ) == > " + den)
  println(" den := "+tfOfWord+" + ( "+k1+" *  "+y+" ) == > " + den)
  println(" num/den :== " +(num/den) )
  println("idfOfWord + (num/den):== "+end)
  println(idfOfWord+" + ("+num+"/"+den+"):== "+end)
  println("valore finale :== "+end)
  println("\n\n\n")

*/