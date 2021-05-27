import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import scala.math.log

class OkapiBM25(dbRDD: RDD[(Int, Array[String])], q: Seq[String]) extends Serializable {

  /*
  |--------------------------------------------------------------------------
  | OkapiBM25 CALCULATION
  |--------------------------------------------------------------------------
  */

  private def calculateOkapiBM25(avgDocLen: Double, tfOfWord: Int, idfOfWord: Double, lengthOfWord: Int): Double = {

    val k1 = 1.5 //va da 1.2 a 2.0
    val b = 0.75
    val num = tfOfWord * (k1 + 1)
    val x = b * (lengthOfWord / avgDocLen)
    val y = (1 - b) + x
    val den = tfOfWord + (k1 * y)
    val result = idfOfWord + (num / den)

    result
  }

def getBM25(): RDD[(Int, Double)]  = {

  //(id, docLen, keyword,tf ) i.e.  ((1,6,spark) 2)
  val tf = dbRDD.flatMap(row => {
    //Seq[((Int, Int, String), Int)] =
  q.map(keyword => {
      ((row._1, row._2.length, keyword), row._2.filter(word => word == keyword).length)
    })
  }).persist()

  //elem is (keword, df ) i.e. el = (keyword,1) if keyword occurs in 1 doc
  //persist not needed because we already have a lazy approach so the computation starts in idf.collectAsMap()
  val df = tf.filter( el => el._2 > 0).map( row =>  (row._1._3 , 1) ).reduceByKey(_+_)

  // ((docId,1),docLength). Persist to avoid two computations
  val docLengths = tf.map( row => ((row._1._1, 1),row._1._2)).persist()

  // alternative to docLengths.count() method.
  val numberOfDocs =  docLengths.reduce((x,y) => ((0, x._1._2 + y._1._2), 0))._1._2

 //i.e. ( word = java , idf = 0.6931471805599453 )
  val idf = df.map( tuple => (tuple._1, calcIDFFunc( tuple._2, numberOfDocs ) ) )
  //df.mapValues(v => calcIDFFunc( v, numberOfDocs ))

  //we collect inside the master memory the entire rdd of idf because it is as the same size as the number of keyword of the query
  val allIdf: collection.Map[String, Double] = idf.collectAsMap()

  val totLen = docLengths.reduce( (x, y) => (x._1, x._2 + y._2) )._2
  val avg: Double = totLen.toDouble / numberOfDocs
  // input tf el = ((docId, docLength, keyword), tf) then we added idf field
  val scores= tf.map(el => {
    ( el._1._1,
      calculateOkapiBM25( avg, el._2, allIdf.getOrElse(el._1._3, 0.0), el._1._2 ))
  }).reduceByKey((x,y) => x + y)

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


/*println("PRINT OF tf: ")
tf.collect().map(row => {
  println(s"id,word = ${row._1} | count = ${row._2} ")
})

println("PRINT OF df: ")
df.collect().map(row => {
  println(s"word = ${row._1} | count = ${row._2} ")
})

println("PRINT OF idf: ")
idf.collect().map(row => {
  println(s"word = ${row._1} | count = ${row._2} ")
})

println("PRINT OF docLengths: ")
docLengths.collect().map(row => {
  println(s"id = ${row._1} | length = ${row._2} ")
})*/
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
