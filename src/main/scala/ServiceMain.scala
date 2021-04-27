import java.lang.ClassCastException

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{coalesce, col, explode}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

//DONE: pre processing db con flag.
//TODO: data pre processing versione RDD.(M)
//DONE: rinominare il file programmaticamente.
//DONE: 1. sorting decrescente e take n 2.(DONE) output finale con titolo e testo dei primi n file trovati. (S) (DONE)
//DONE: lettura del DB da file s3.


object ServiceMain {

  // LOCAL MARY
  /*
  val path = ""
  val pathWikiClean = "wikiclean.json"
  val pathIdTitleTextDB = "idTitleTextDB.json"
  val pathWikiDB = "wikidb.json"
  val pathOutput = "outputTests/"
  */

  // LOCAL SAL
 /* val whereReadingDB = "C:\\Users\\Salvo\\GitHub\\data\\jsonFiles\\"
  val wherePutOutput = "outputFolder\\"
  val pathOfWikiClean = wherePutOutput + "wikidb\\"+"wikiclean.json"
  val pathOfIdTitleTextDB =  wherePutOutput+"idTitleTextDB\\"+"idTitleTextDB.json"
 */

  //AWS
  //la prima viene usata nel preprocess quando viene letto uno degli n file.
  val whereReadingDB = "s3://sal1/"

  //la seconda viene usata ogni volta che si scrive sul bucket.: si scrive in 3 occasioni e tutti usano wherePutOutput come root.
  //1. alla fine del preprocess per salvare le rdd preprocessate
  //2. si rinominano i file dopo la loro creazione dando come indirizzo quello che poi genera path of WikiClean e IdTitleTextDB
  //3. infine l'ultima scrittura genera file che però non vengono rinominati ed è l'output finale

  val wherePutOutput = "s3://sal1/outputFolder/"

  //queste ultime due sono i path che vengono usati per leggere i dati in caso in cui si evita il preprocess
  val pathOfWikiClean = wherePutOutput + "wikidb/"+"wikiclean.json"
  val pathOfIdTitleTextDB =  wherePutOutput+"idTitleTextDB/"+"idTitleTextDB.json"


  /*
  *  !!!!!!!!!!!!! considerazioni da dire a marianna !!!!!!!!1
  * per renderre il prog scalabile dovremmo preprocessare tutti i file e poi passare come parametro il numero di file da leggere.
  * Secondo me per dimostrare la prof che funziona tutto ci basta lavorare non su 80GB ma anche su 1GB.
  * Per questo motivo direi di fare l'upload di tutti i file json, poi ho inserito anche la possibilità di dare il comando
  *  --numFile=3 per esempio per poter leggere 3 file.
  *
  * */

  /*
 val path = "s3://scpmarysal/"
 val pathWikiClean = path+"wikiclean.json"
 val pathIdTitleTextDB = path+"idTitleTextDB.json"
 val pathWikiDB = path+"preProcessedDB"
 val pathOutput = path+"eccoIl30/"
*/
  /*
    ReadXMLfromS3(spark: SparkSession, xmlFile: String): DataFrame = {

     val revisionStruct = StructType(
       StructField("text", StringType, true) :: Nil)

     val schema = StructType(
       StructField("title", StringType, true) ::
         StructField("id", IntegerType, false) ::
         StructField("revision", revisionStruct, true) :: Nil)

     val df = spark.read.format("com.databricks.spark.xml")
       .option("rootTag", "mediawiki")
       .option("rowTag", "page")
       .schema(schema)
       .xml(xmlFile)

     df.selectExpr("id", "title", "revision.text")

   }
 */

 def generateOutput(idTitleTextDB: DataFrame, source: Array[SimpleTuple], spark: SparkSession) = {
   val df = idTitleTextDB.collect().par
   val mapsource = source.map( x=> (x.idOfTheDoc, x.value) ).toMap.par
   val res = for{
    row <-df
    idDF = row(1).toString.toLong
    pair <- mapsource
    idS = pair._1
  }yield{
     synchronized{
       if ( idDF == idS) {
         (idS, pair._2 ,row(2).toString, row(0).toString )
       }else{
         (0,0.0,"","")
       }
     }
  }

  val x = res.filterNot( x => x == (0,0.0,"","") );
   x
 }

 def generateParDF(idTitleTextDB: DataFrame, source: Array[(Int,Double)], spark: SparkSession) = {

   /* TEMPO DI ESECUZIONE di questa FUNZIONE
     {"id":"ParDF","text":"Elapsed time: 259ms"}
     {"id":"ParDF","text":"Elapsed time: 0sec"}
    */
   val ids = source.map(el => el._1.toString)
   idTitleTextDB.filter(col("id").isin(ids: _*))
 }

 def block_of_code(tokenizedPreprocessedDB: RDD[(Int, Array[String])], idTitleTextDB: DataFrame, myQuery: Array[String], sparkSession: SparkSession): Unit = {

  // tokenizedPreprocessedDB.show(false)
   val okapi = new ParOkapiBM25(tokenizedPreprocessedDB, myQuery)
   val scores: RDD[(Int, Double)] = okapi.getBM25()

   //val scoresArray = scores.collect()
   //Sorting.parMergeSort(scoresArray, 2);
   object PairOrdering extends Ordering[(Int,Double)] {
     def compare(a: (Int,Double), b:(Int,Double)) = a._2 compare b._2
   }

   val n = 10
   lazy val firstN = scores.top(n)( PairOrdering ); //fa il contrario di take ordering e torna i primi 10 con score più alto direttamente dalle RDD.. consuma molto meno memoria sul driver.
   firstN map (println(_))
   lazy val result = generateParDF(idTitleTextDB, firstN, sparkSession)
   //lazy val result = generateOutput(idTitleTextDB, firstN, sparkSession).seq
   //sc.parallelize(result).coalesce(1).saveAsTextFile("outputTests/output");
   result.coalesce(1).write.mode("overwrite").format("json").save(wherePutOutput+"/output")

 }

 def readFullDBFromJson(path: String, spark: SparkSession): Array[DataFrame] = {

    val dfDB = spark.read
      .option("multiline", "true")
      .json(path)

    val arrDF = new Array[DataFrame](2)

    val arrDB = dfDB.selectExpr("mediawiki.page")
    val tmpDF = arrDB.withColumn("page", explode(col("page")))

    arrDF(0) = tmpDF.selectExpr("page.id", "page.revision.text.content")
    arrDF(1) = tmpDF.selectExpr("page.id", "page.title", "page.revision.text.content")

    arrDF

  }

 def readFullDBFromJsonOld(path: String, spark: SparkSession): Array[DataFrame] = {

   val dfDB = spark.read
     .option("multiline", "true")
     .json(path)

   val arrDB = dfDB.selectExpr("mediawiki.page")

   val arrDF = new Array[DataFrame](2)
   val tmpDF = arrDB.withColumn("page", explode(col("page")))
   arrDF(0) = tmpDF.selectExpr("page.id", "page.revision.text.__text")
   arrDF(1) = tmpDF.selectExpr("page.id", "page.title", "page.revision.text.__text")

   arrDF

 }

 def readPreprocessedDBFromJson(path: String, spark: SparkSession): DataFrame = {
   val dfDB = spark.read
     .json(path)
   dfDB
 }

 /*
 * @args param options example
 * -preprocess false/true keywords
 *
 * */
 //def fromDFtoRDD[T](sc: SparkContext, tokenizedPreprocessedDB: DataFrame) : RDD[T] = { volevo farla in questo modo vedi dopo come fare}
 def fromDFtoRDD(sc: SparkContext, tokenizedPreprocessedDB: DataFrame) =
   sc.parallelize(  tokenizedPreprocessedDB.collect() )
     .map( row => (
       row.getLong(0).toInt,
       row.get(1).asInstanceOf[mutable.WrappedArray[String]].toArray[String]
     ))


 def main(args: Array[String]): Unit = {

   val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
   val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
   spark.sparkContext.setLogLevel("WARN")
   val sc = spark.sqlContext.sparkContext

  // OFFICIAL VERSION
   var tokenizedPreprocessedDB: DataFrame = null
   var idTitleTextDB: DataFrame = null

   println("args => ")
   args.foreach( x => print(" " +x))

   def checkandgetArgue(commands: Array[String]) = {

     //ci devono essere 3 comandi se non ci sono questi 3 la query non può continuare.
     //il modello corretto di query sarebbe
     // --preprocess=true/false --numFile=N --query=""
     var prep: Boolean = null
     val prepIndex = commands.indexOf("--preprocess=")

     prepIndex match {
       case -1 => throw new IllegalArgumentException("--preprocess command is missing")
       case _=>  prep = commands(prepIndex+1) match {
           case "true" => true
           case "false" =>false
           case _ =>  throw new IllegalArgumentException("--preprocess can have only boolean value")
         }
     }

     val nfIndex = commands.indexOf("--numFile=")
     var numfile: Int = null

     nfIndex match {
       case -1 => throw new IllegalArgumentException("--numFile command is missing")
       case _ => numfile = commands(nfIndex+1) match {
         case value => try value.toInt
             catch{
               case ClassCastException =>  throw new IllegalArgumentException("--numFile can have only integer value ")
             }
       }
     }

     val queryIndex = commands.indexOf("--query=")
     var query : String = null
     queryIndex match {
       case -1 => throw new IllegalArgumentException("--query command is missing")
       case _ => query = commands(queryIndex+1) match {
         //case "the query value is a words inside the upper quotes"
       //case _ =>  throw new IllegalArgumentException("--query can have only a sentence between upper quotes \" \" ")
         
       }
     }



     (prep,numfile,query)
   }



   //SPLIT ARGS
   //if check is necessary to avoid null pointer exception
   val (command,query) = if(args.length > 2 ) args.splitAt(2) else new Array[String](2) -> new Array[String](0)

   /* JSON TO DF  */
   (command(0), command(1) )match {
     case ("--preprocess","false") => {
       // READ PREPROCESSED DB
       tokenizedPreprocessedDB = readPreprocessedDBFromJson(pathOfWikiClean, spark)
       idTitleTextDB = readPreprocessedDBFromJson(pathOfIdTitleTextDB, spark)

     }
     case ("--preprocess","true")  => {
       // PREPROCESS DB and WRITE IT on S3
       val fullDB = readFullDBFromJson(whereReadingDB + "wikiSplitted_3.json", spark)
       val toBePreprocessedDB = fullDB(0)
       idTitleTextDB = fullDB(1)

       //PREPROCESS FULL DB
       val preProcessData = new DataPreprocessing(toBePreprocessedDB)
       tokenizedPreprocessedDB = preProcessData.preProcessDF()

       //WRITE PREPROCESSED DBs TO A FILE
       //lo mette dentro la cartella idTitleTextDB al percorso
       idTitleTextDB.write.mode("overwrite").format("json").save(wherePutOutput + "idTitleTextDB")
       tokenizedPreprocessedDB.write.mode("overwrite").format("json").save(wherePutOutput + "wikidb")

       //RENAME DB FILE
       //renameDBFile("wikiclean.json",  "preprocessedDB/", "scpmarysal", "us-east-1") //? non torna preprocessedDB
       renameDBFile("wikiclean.json", wherePutOutput + "wikidb/", "scpmarysal", "us-east-1") //? non torna il preprocessedDB preprocessedDB
       renameDBFile("idTitleTextDB.json", wherePutOutput+"idTitleTextDB/", "scpmarysal", "us-east-1") //?
     }

     case _ => throw new IllegalArgumentException("--preprocess is missing or was not recognized with value true or false")
   }

   val userInput = query.reduce((x, y) => x + " " + y)

   val seqQuery = Seq((0, userInput))
   val queryDF = spark.createDataFrame(seqQuery).toDF("id", "content")
   val dbQuery = new DataPreprocessing(queryDF).preProcessDF()
   val queryKeyword = dbQuery.select("words_clean").collect()
   val myQuery = queryKeyword(0).get(0).asInstanceOf[mutable.WrappedArray[String]].toArray[String]

   val preProcessedRDD = fromDFtoRDD(sc,tokenizedPreprocessedDB)
   //preProcessedRDD.take(10).foreach( x=> println( x._1.toString + " --- "+x._2.toString ))
   time(block_of_code(preProcessedRDD, idTitleTextDB, myQuery, spark), spark, "TotalExec")

   // END OFFICIAL VERSION

  // TEST VERSION
   /*
    val testSenteces = Seq(
      (1, Array("hi", "heard", "spark", "think", "spark", "beautiful")),
      (2, Array("java", "java", "spark", "spark", "nosql", "sql")),
      (3, Array("logistic", "regression", "models", "neat", "spark"))
    )

    val preProcessed2DB = sc.parallelize(testSenteces)

    val myQuery =  Array("logistic", "safdsl", "regression" , "dsafkj", "models" ,"suca" ,"neat" ,"developed", "good" ,"nosql", "program")
    //val myQuery = Array("spark", "java", "wow")
   val idTitleTextDB = null
   time(block_of_code(preProcessed2DB, idTitleTextDB, myQuery, spark), spark, "TotalExec")
   // END TEST VERSION
  */
 }

 def time[R](block: => R, spark: SparkSession, name: String): R = {

   val t0 = System.nanoTime()
   val result = block // call-by-name
   val t1 = System.nanoTime()

   val elapsedTimeSentence = Seq(
     (name, "Elapsed time: " + (t1 - t0) / 1000000 + "ms"),
     (name, "Elapsed time: " + (t1 - t0) / 1000000000 + "sec"),
   )
   println("elapsed time =>  \n"+elapsedTimeSentence.map( x => "\n "+x._2))

   val elapsedTimeSentenceDataFrame = spark.createDataFrame(elapsedTimeSentence).toDF("id", "text")
   elapsedTimeSentenceDataFrame.coalesce(1).write.mode("overwrite").format("json").save(wherePutOutput + "timeOutput")

   result
 }

 def renameDBFile(newfileName: String, folder: String, bucketName: String, clientRegion: String): Unit = {

   val s3 = AmazonS3ClientBuilder.standard.withRegion(clientRegion).build

   val x = s3.listObjectsV2(bucketName, folder)
   val y = x.getObjectSummaries()

   y.forEach(r => {
     val fileName = r.getKey
     if (fileName.contains("part-"))
       s3.copyObject(bucketName, fileName, bucketName, newfileName)
     s3.deleteObject(bucketName, fileName)
   })

   /*
   val req = new PutObjectRequest("scpmarysal", "test", "test.json")
   s3.putObject(req)
*/

   /*
   s3.putObject("scpmarysal", "test", "test.json")
   s3.copyObject("scpmarysal", "test", "scpmarysal", "test2")
   s3.deleteObject("scpmarysal", "test")

   val clientRegion = Regions.DEFAULT_REGION
   val bucketName = "scpmarysal"

   try {
     val s3Client = AmazonS3ClientBuilder.standard.withRegion(clientRegion).build
     // Get a list of objects in the bucket, two at a time, and
     // print the name and size of each object.
     val listRequest = new ListObjectsRequest().withBucketName(bucketName)
     var objects = s3Client.listObjects(listRequest)
     while ( {
       true
     }) {
       val summaries = objects.getObjectSummaries
       summaries.forEach(summary => {
         val testSenteces3 = Seq(
           (1, s"Object ${summary.getKey} retrieved with size ${summary.getSize} \n")
         )

         val sentence2DataFrame = spark.createDataFrame(testSenteces3).toDF("id","text")

         sentence2DataFrame.coalesce(1).write
           .mode("overwrite")
           .format("json")
           .save("s3://scpmarysal/awsTestOutput/")
       }

       )

       if (objects.isTruncated) objects = s3Client.listNextBatchOfObjects(objects)
     }
   } catch {
     case e: AmazonServiceException =>
       // The call was transmitted successfully, but Amazon S3 couldn't process
       // it, so it returned an error response.
       e.printStackTrace()
     case e: SdkClientException =>
       // Amazon S3 couldn't be contacted for a response, or the client
       // couldn't parse the response from Amazon S3.
       e.printStackTrace()
   }
*/
 }

}

case class SimpleTuple(idOfTheDoc: Long, value: Double) {
 override def toString: String = this.idOfTheDoc + ",\t" + this.value
 def compareTo(x: SimpleTuple) = this.value - x.value
}