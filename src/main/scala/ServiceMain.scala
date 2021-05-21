import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object ServiceMain {

  private var whereReadingDB = ""
  private var wherePutOutput = ""

  def generateParDF(idTitleTextDB: DataFrame, source: Array[(Int, Double)], spark: SparkSession) = {
    /* TEMPO DI ESECUZIONE di questa FUNZIONE
     {"id":"ParDF","text":"Elapsed time: 259ms"}
     {"id":"ParDF","text":"Elapsed time: 0sec"}
    */
    def ids = source.map ( el => el._1.toString )
    idTitleTextDB.filter ( col ( "id" ).isin ( ids: _* ) )
  }

  def block_of_code(args: Array[String], spark: SparkSession, sc: SparkContext): Unit = {

    // OFFICIAL VERSION
    var tokenizedPreprocessedDB: DataFrame = null
    var idTitleTextDB: DataFrame = null
    var toBePreprocessedDB: DataFrame = null

    val (prep, file_path, query, partitionNumber, in ) = check_and_getArgues ( args )

    println ( "preprocessing value is => " + prep )
    println ( "filepath value is => " + file_path )
    println ( "query value is => " + query )
    println ( "partition number is => "+partitionNumber )
    println( "reading path input as bucket/input"+in)

    wherePutOutput = file_path +"/output/"
    whereReadingDB = file_path + "/input"+in

    prep match {
      case true  => {

        // PREPROCESS DB and WRITE IT on S3
        val tempFullDB = readFullDBFromJson ( whereReadingDB, spark )
        toBePreprocessedDB = tempFullDB._1 repartition partitionNumber
        idTitleTextDB = tempFullDB._2 repartition partitionNumber

        //PREPROCESS FULL DB
        val preProcessData = new DataPreprocessing ( toBePreprocessedDB )
        tokenizedPreprocessedDB = preProcessData.preProcessDF()

        //WRITE PREPROCESSED DBs TO A FILE
        idTitleTextDB.write.mode ( SaveMode.Overwrite ).format ( "json" ).save ( wherePutOutput + "idTitleTextDB/" )
        tokenizedPreprocessedDB.write mode ( SaveMode.Overwrite ) format ( "json" ) save ( wherePutOutput + "wikidb/" )
      }
      case false => {
        // READ PREPROCESSED DB working code
        tokenizedPreprocessedDB = readPreprocessedDBFromJson ( wherePutOutput + "wikidb/", spark )
        idTitleTextDB = readPreprocessedDBFromJson ( wherePutOutput + "idTitleTextDB/", spark )

        ///test version
        /**idTitleTextDB = null;
        val testSenteces = Seq(
          (1L, Array("hi", "heard", "spark", "think", "spark", "beautiful")),
          (2L, Array("java", "java", "spark", "spark", "nosql", "sql")),
          (3L, Array("logistic", "regression", "models", "neat", "spark"))
        )
        val preProcessed2DB = sc.parallelize(testSenteces)
        //val myQuery =  Array("logistic", "safdsl", "regression" , "dsafkj", "models" ,"suca" ,"neat" ,"developed", "good" ,"nosql", "program")
        //to pas as args => -q ="spark java wow"
        tokenizedPreprocessedDB = spark.createDataFrame(preProcessed2DB)*/
      }
    }

    val seqQuery = Seq ( (0, query) )
    val queryDF = spark.createDataFrame ( seqQuery ).toDF ( "id", "content" )
    val dbQuery = new DataPreprocessing ( queryDF ).preProcessDF ()
    val queryKeyword = dbQuery.select ( "words_clean" ).collect ()
    val myQuery = queryKeyword ( 0 ).get ( 0 ).asInstanceOf[mutable.WrappedArray[String]].toArray [String]

    val preProcessedRDD = fromDFtoRDD ( tokenizedPreprocessedDB )
    val okapi = new OkapiBM25 ( preProcessedRDD, myQuery )
    val scores: RDD[(Int, Double)] = okapi.getBM25 ()

    object PairOrdering extends Ordering[(Int, Double)] {
      def compare(a: (Int, Double), b: (Int, Double)) = a._2 compare b._2
    }

    val n = 10
    val firstN = scores.top ( n )( PairOrdering ); //fa il contrario di take ordering e torna i primi 10 con score più alto direttamente dalle RDD.. consuma molto meno memoria sul driver.
    firstN map (println ( _ ))
    spark.createDataFrame ( firstN ).coalesce ( 1 ).write.mode ( "overwrite" ).format ( "json" ).save ( wherePutOutput + "/scores" )

    val result = generateParDF ( idTitleTextDB, firstN, spark )
    result.write.mode ( "overwrite" ).format ( "json" ).save ( wherePutOutput + "/output" )

  }
  def readFullDBFromJson(path: String, spark: SparkSession) = {
    println ( "reading files at " + path )

    val dfDB = spark.read
      .option ( "multiline", "true" )
      .json ( path )

    println ( "number of gotten files is " + dfDB.count () )

    def arrDB = dfDB.selectExpr ( "mediawiki.page" )
    val tmpDF = arrDB.withColumn ( "page", explode ( col ( "page" ) ) )

    val dfToPreprocess = tmpDF.selectExpr ( "page.id", "page.revision.text.content" )
    val dfToOutput = tmpDF.selectExpr ( "page.id", "page.title", "page.revision.text.content" )

    (dfToPreprocess, dfToOutput)
  }
  def readPreprocessedDBFromJson(path: String, spark: SparkSession): DataFrame =  spark.read.json ( path )

  def fromDFtoRDD(tokenizedPreprocessedDB: DataFrame) =
    tokenizedPreprocessedDB.rdd
      .map ( row => (
        row.getLong ( 0 ).toInt,
        row.get ( 1 ).asInstanceOf[mutable.WrappedArray[String]].toArray [String]
      ) )


  /*
  * example of accepted arguments
  * -f="s3://mybucket/" -p=true -i=2 -c=48 -q="House buy and sell"
  * */
  def check_and_getArgues(commands: Array[String]) = {

    var preprocess: Boolean = false //default value
    var file_path: String = "" //default value
    var query: String = ""     //default value
    var partition: Int = 6     //default value
    var input: String = ""     //default value
    commands.foreach ( f = arg =>
      arg.substring ( 0, 2 ) match {
        case "-p" =>
          preprocess = arg.substring ( 3, arg.length ) match {
            case "true" => true
            case "false" => false
            case _ => throw new IllegalArgumentException ( "-p can have only true or false value" )
          }
        case "-f" => file_path = arg.substring ( 3, arg.length )
        case "-q" => query = arg.substring ( 3, arg.length )
        case "-c" => try partition = arg.substring ( 3, arg.length ).toInt catch {
          case _: NumberFormatException => throw new IllegalArgumentException ( "-c can have only integer values" )
        }
        case "-i" => {
          val x: String = arg.substring ( 3, arg.length )
          input = x match {
            case "2" => x
            case "3" => x
            case _ => throw new IllegalArgumentException ( "-i can have only 2 or 3 values" )
          }
        }
        case _ => throw new IllegalArgumentException ( "Errors on arguments" )
      }
    )

    (preprocess, file_path, query, partition, input)
  }

  def main(args: Array[String]): Unit = {

    def conf = new SparkConf().setAppName ( this.getClass.getName ).set("spark.cleaner.ttl","100000")//.setMaster ( "local[*]" )
    lazy val spark: SparkSession = SparkSession.builder.config ( conf ).getOrCreate ()

    spark.sparkContext.setLogLevel ( "WARN" )
    val sc = spark.sqlContext.sparkContext
    time ( block_of_code ( args, spark, sc ), spark, "TotalExec" )
  }

  def time[R](block: => R, spark: SparkSession, name: String): R = {
    val t0 = System.nanoTime ()
    val result = block // call-by-name
    val t1 = System.nanoTime ()

    val elapsedTimeSentence = Seq (
      (name, "Elapsed time: " + (t1 - t0) / 1000000 + "ms"),
      (name, "Elapsed time: " + (t1 - t0) / 1000000000 + "sec"),
    )
    println ( "elapsed time =>  \n" + elapsedTimeSentence.map ( x => "\n " + x._2 ) )

    val elapsedTimeSentenceDataFrame = spark.createDataFrame ( elapsedTimeSentence ).toDF ( "id", "text" )
    elapsedTimeSentenceDataFrame.coalesce ( 1 ).write.mode ( "overwrite" ).format ( "json" ).save ( wherePutOutput + "timeOutput" )
    result
  }

}

/*
 def renameDBFile(newfileName: String, folder: String, sal1: String, clientRegion: String): Unit = {

   val s3 = AmazonS3ClientBuilder.standard.withRegion(clientRegion).build
   val x = s3.listObjectsV2(sal1, folder)
   val y = x.getObjectSummaries()

   y.forEach(r => {
     val fileName = r.getKey
     if (fileName.contains("part-"))
       s3.copyObject(sal1, fileName, sal1, newfileName)

     s3.deleteObject(sal1, fileName)
   })

   /*
    val req = new PutObjectRequest("sal1", "test", "test.json")
    s3.putObject(req)
   */
   /*
   s3.putObject("sal1", "test", "test.json")
   s3.copyObject("sal1", "test", "sal1", "test2")
   s3.deleteObject("sal1", "test")

   val clientRegion = Regions.DEFAULT_REGION
   val sal1 = "sal1"

   try {
     val s3Client = AmazonS3ClientBuilder.standard.withRegion(clientRegion).build
     // Get a list of objects in the bucket, two at a time, and
     // print the name and size of each object.
     val listRequest = new ListObjectsRequest().withsal1(sal1)
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
           .save("s3://sal1/awsTestOutput/")
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

*/
/*def readFullDBFromJsonOld(path: String, spark: SparkSession): Array[DataFrame] = {

   val dfDB = spark.read
     .option("multiline", "true")
     .json(path)

   val arrDB = dfDB.selectExpr("mediawiki.page")

   val arrDF = new Array[DataFrame](2)
   val tmpDF = arrDB.withColumn("page", explode(col("page")))
   arrDF(0) = tmpDF.selectExpr("page.id", "page.revision.text.__text")
   arrDF(1) = tmpDF.selectExpr("page.id", "page.title", "page.revision.text.__text")

   arrDF
 }*/
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
/*
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
*/