import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.{GenSeq, mutable}
import scala.math.log

class ParOkapiBM25(dbRDD: RDD[(Int, Array[String])], q: Seq[String]) extends Serializable {

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


def getBM25() :  RDD[(Int, Double)]  = {

  /*
  //OK MA SENZA KEYWORDS MANCANTI
  //(id,docLength,word , 1)
  val unfoldToCountTF = dbRDD.flatMap( row => {
   row._2.filter(word => q.indexOf(word) != -1)
      .map(word => ((row._1, row._2.length, word),1))
  })
   */
  //unfoldToCountTF = RDD[((Int, Int, String), Int)]
  val tf = dbRDD.flatMap(row => {
    val x: Seq[((Int, Int, String), Int)] = q.map(keyword => {
      if(row._2.indexOf(keyword) != -1)
        ((row._1, row._2.length, keyword), row._2.filter(word => word == keyword).length)
      else
        ((row._1, row._2.length, keyword),0)
    })
    x
  })
  // DF = num di documenti in cui compare la keyword.
  //  Quindi risultato deve essere (keyword, num di documenti in cui compare)
  //val df = tf.map( row =>  (row._1._3 , 1) ).reduceByKey(_+_)
  //if count di (keyword,id) > 0 --> (keyword,1)
  val df = tf.filter( el => el._2 > 0).map( row =>  (row._1._3 , 1) ).reduceByKey(_+_)

  /*
  println("PRINT OF tf: ")
  tf.collect().map(row => {
    println(s"id,word = ${row._1} | count = ${row._2} ")
  })

  println("PRINT OF df: ")
  df.collect().map(row => {
    println(s"word = ${row._1} | count = ${row._2} ")
  })
*/
  /*
  println("PRINT OF idf: ")
  idf.collect().map(row => {
    println(s"word = ${row._1} | count = ${row._2} ")
  })
*/
  val docLengths = dbRDD.map( row => {
    ((row._1, 1),row._2.length)
  })

  /*
  println("PRINT OF docLengths: ")
  docLengths.collect().map(row => {
    println(s"id = ${row._1} | length = ${row._2} ")
  })
*/
  val totLen = docLengths.reduce( (x, y) => (x._1, x._2 + y._2) )._2
  // println(s"PRINT OF totLen: $totLen")

  // ALTERNATIVA A docLengths.count()
  val numberOfDocs =  docLengths.reduce((x,y) => ((0, x._1._2 + y._1._2), 0))._1._2
  //println(s"PRINT OF numberOfDocs: $numberOfDocs")

  val avg: Double = totLen.toDouble / numberOfDocs
  //println(s"PRINT OF avg: $avg")
  //Sarà sempre piccolo perchè conterrà solo le keyword della query che compaiono nei doc
  val idf = df.map( tuple => (tuple._1, calcIDFFunc( tuple._2, numberOfDocs ) ) )
  val allIdf = idf.collectAsMap()
  // el = ((docId, docLength, keyword), tf)
  val fullData = tf.map(el => {
    //(docId, docLength, keyword, tf, idf)
    (el._1._1, el._1._2, el._1._3, el._2, allIdf.getOrElse(el._1._3, 0.0) )
  })
  /*
  println("PRINT OF fullData: ")
  fullData.collect().map(row => {
    println(s"id = ${row._1} | docLength = ${row._2} | | keyword = ${row._3} | tf = ${row._4} | idf = ${row._5} ")
  })
*/
  val scores= fullData.map(el => {
    ( el._1,
      calculateOkapiBM25( avg, el._4, el._5, el._2 ))
  }).reduceByKey((x,y) => x + y)
  /*
  println("PRINT OF scores: ")
  scores.collect().map(row => {
    println(s"id = ${row._1} | score = ${row._2} ")
  })
*/
  // docLengths.foreachPartition()

 //id , array (key,1)
  // id, key, 1
  // id, key, 1
  // id, key, 1
  /*
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
*/
  /*
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
    }).toArray

  scores
*/
  scores
}
  private def calcIDFFunc(df: Double, size: Long): Double = {
    log((size.toDouble + 1) / (df + 1))
  }
  /*
  //return: calcolo dei tf di q su tutti i documenti
  def calcTF(df: DataFrame, word: String): DataFrame = {
    df.groupBy("id").agg(count(when(df("page_tokens") === word, 1)).as(s"cnt_$word"))
  }

  //return: calcolo tutte lunghezze dei documenti
  //input: dataframe processato
  /*
  def calcPageLenght(): DataFrame = {
    val lengthUdf = udf { (page_words: mutable.WrappedArray[String]) => page_words.length }
    dataFrame.withColumn("doc_length", lengthUdf(dataFrame("words_clean")))
  }

   */

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
*/
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