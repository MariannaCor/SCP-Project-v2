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
/*
  // LOCAL SAL
  val path = ""
  val pathWikiClean = "wikiclean.json"
  val pathIdTitleTextDB = "idTitleTextDB.json"
  val pathWikiDB = "wikidb.json"
  val pathOutput = "outputTests/"

*/

  //AWS
  val path = "s3://scpmarysal/"
  val pathWikiClean = path+"wikiclean.json"
  val pathIdTitleTextDB = path+"idTitleTextDB.json"
  val pathWikiDB = path+"preProcessedDB"
  val pathOutput = path+"eccoIl30/"

  /*
    def ReadXMLfromS3(spark: SparkSession, xmlFile: String): DataFrame = {

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

    result.coalesce(1).write
      .mode("overwrite")
      .format("json")
      .save("outputTests/output")

  }


  def readFullDBFromJson(path: String, spark: SparkSession): Array[DataFrame] = {

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
    sc.parallelize(tokenizedPreprocessedDB.collect())
      .map( row => (
        row.getString(0).toInt, row.get(1).asInstanceOf[mutable.WrappedArray[String]].toArray[String])
      )




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

    //SPLIT ARGS
    //if check is necessary to avoid null pointer exception
    val (command,query) = if(args.length > 3) args.splitAt(2) else new Array[String](2) -> new Array[String](0)

    /* JSON TO DF  */
    (command(0), command(1) )match {
      case ("--preprocess","false") => {
        // READ PREPROCESSED DB
        tokenizedPreprocessedDB = readPreprocessedDBFromJson(pathWikiClean, spark)
        idTitleTextDB = readPreprocessedDBFromJson(pathIdTitleTextDB, spark)
      }
      case ("--preprocess","true")  => {
        // PREPROCESS DB and WRITE IT on S3
        val fullDB = readFullDBFromJson(path + "wikidb.json", spark)
        tokenizedPreprocessedDB = fullDB(0)
        idTitleTextDB = fullDB(1)

        //PREPROCESS FULL DB
        val preProcessData = new DataPreprocessing(tokenizedPreprocessedDB)
        val preProcessedDB = preProcessData.preProcessDF()

        idTitleTextDB.write.mode("overwrite").format("json").save(path + "idTitleTextDB")

        //WRITE PREPROCESSED DB TO A FILE
        preProcessedDB.write
          .mode("overwrite")
          .format("json")
          .save(pathWikiDB)

        //RENAME DB FILE
        renameDBFile("wikiclean.json", "preProcessedDB/", "scpmarysal", "us-east-1")
        renameDBFile("idTitleTextDB.json", "idTitleTextDB/", "scpmarysal", "us-east-1")
      }

      case _ => throw new IllegalArgumentException("--preprocess is missing or was not recognized with value true or false")
    }

    val userInput = query.reduce((x, y) => x + " " + y)

    val seqQuery = Seq((0, userInput))
    val queryDF = spark.createDataFrame(seqQuery).toDF("id", "__text")
    val dbQuery = new DataPreprocessing(queryDF).preProcessDF()
    val queryKeyword = dbQuery.select("words_clean").collect()
    val myQuery = queryKeyword(0).get(0).asInstanceOf[mutable.WrappedArray[String]].toArray[String]

    val preProcessedRDD = fromDFtoRDD(sc,tokenizedPreprocessedDB)
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

    elapsedTimeSentenceDataFrame.coalesce(1).write
      .mode("overwrite")
      .format("json")
      .save(pathOutput + "timeOutput")

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