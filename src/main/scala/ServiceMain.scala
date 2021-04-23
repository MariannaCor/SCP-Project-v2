import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

//DONE: pre processing db con flag.
//TODO: persist su parOkapi. (S&M)
//TODO: sync su parOkapi. (S&M)
//TODO: data pre processing versione RDD.(M)
//DONE: rinominare il file programmaticamente.
//DONE: 1. sorting decrescente e take n 2.(DONE) output finale con titolo e testo dei primi n file trovati. (S) (DONE)
//DONE: lettura del DB da file s3.


object ServiceMain {

  // LOCAL MARY
  val path = ""
  val pathWikiClean = "wikiclean.json"
  val pathIdTitleTextDB = "idTitleTextDB.json"
  val pathWikiDB = "wikidb.json"
  val pathOutput = "outputTests/"

  /*
  // LOCAL SAL
  val path = ""
  val pathWikiclean = ""
  val pathWikiDB = ""
  val pathTitleDB = ""
  val pathOutput = ""
  */

  /*
  //AWS
  val path = "s3://scpmarysal/"
  val pathWikiClean = "s3://scpmarysal/wikiclean.json"
  val pathIdTitleTextDB = "s3://scpmarysal/idTitleTextDB.json"
  val pathWikiDB = "s3://scpmarysal/preProcessedDB"
  val pathOutput = "s3://scpmarysal/eccoIl30/"
   */

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

  /*
  *
  *   A Dataset is a distributed collection of data.
  *   Dataset in an interface that provides the benefits of RDDs
  *   (strong typing, ability to use powerful lambda functions)
  *   with the benefits of Spark SQL’s optimized execution engine.
  *   quindi ogni volta che creiamo un dataframe è un RDD.
  *
  * */

  /*
  def generateOutput(textDB: DataFrame, titleDB: DataFrame , source: Array[SimpleTuple], sparkSession: SparkSession): Array[(Int, Double, String, String)] = {

    val texts = textDB.collect().par
    val titles = titleDB.collect().par

    val temp = for {
      field <- source.par
      index = titles.indexOf(field.idOfTheDoc)
    } yield {
      if( index > -1 ) {
        (field.idOfTheDoc, field.value, titles(index).getString(1))
      }else{
        (0, 0.0, "")
      }
    }

    val temp: ParArray[(Int,Double,String)] = titles
      .map { field => val id = field.getInt ( 0 ); (field, id) }
      .map { case (field, id) => val index = source.indexOf ( id ); (field, id, index) }
      .map { case (field, id, index) =>
        if( index > -1 ) {
        (id, source ( index ).value, field.getString ( 1 ))
        }else{
          (0, 0.0, "")
        }
      } //se non metto un else non torna lo stesso tipo, torna una volta la tripla una volta UNIT e quindi il primo padre fra i due è ANY


   val result = for{
     elem <- temp.filterNot( x=>  { x._1 == 0 && x._2 == 0.0 && x._3 == "" } )
     id = elem._1;
     index = texts.indexOf(id)
   }yield{
     if(index > -1) (id, elem._2, elem._3, texts(index).toString() ) else (0,0.0,"","")
   } //non è chiaro perchè torna tipo ANY e non la tripla creata

    result.filterNot(
      x=>  { x._1 == 0 && x._2 == 0.0 && x._3 == "" }
    ).toArray
  }

   */

  /*
  def generateParSource(textDB: DataFrame, titleDB: DataFrame , source: Array[SimpleTuple], sparkSession: SparkSession) = {

    val texts = textDB.collect().par
    val titles = titleDB.collect().par

    val temp = for {
      field <- source.par
      index = titles.indexOf(field.idOfTheDoc)
    } yield {
      if( index > -1 ) {
        (field.idOfTheDoc, field.value, titles(index).getString(1))
      }else{
        (0, 0.0, "")
      }
    }

    val result = for{
      elem <- temp.filterNot( x=>  { x._1 == 0 && x._2 == 0.0 && x._3 == "" } )
      id = elem._1;
      index = texts.indexOf(id)
    }yield{
      if(index > -1) (id, elem._2, elem._3, texts(index).toString() ) else (0,0.0,"","")
    } //non è chiaro perchè torna tipo ANY e non la tripla creata

    result.filterNot(
      x=>  { x._1 == 0 && x._2 == 0.0 && x._3 == "" }
    ).toArray
  }

  def generateParTitles(textDB: DataFrame, titleDB: DataFrame , source: Array[SimpleTuple], sparkSession: SparkSession) = {

    val texts = textDB.collect().par
    val titles = titleDB.collect().par

    val sourceMap = source.map( el => {
      (el.idOfTheDoc,el.value)
    }).toMap

    val temp: ParArray[(Int,Double,String)] = for{
      field <- titles
      id = field.getString(0).toInt
      value = sourceMap.getOrElse(id , 0.0 )
    } yield {
      (id, value, field.getString ( 1 ))
    }//se non metto un else non torna lo stesso tipo, torna una volta la tripla una volta UNIT e quindi il primo padre fra i due è ANY


    val result = for{
      elem <- temp.filterNot( x=>  { x._1 == 0 && x._2 == 0.0 && x._3 == "" } )
      id = elem._1;
      index = texts.indexOf(id)
    }yield{
      if(index > -1) (id, elem._2, elem._3, texts(index).toString() ) else (0,0.0,"","")
    } //non è chiaro perchè torna tipo ANY e non la tripla creata

    result.filterNot(
      x=>  { x._1 == 0 && x._2 == 0.0 && x._3 == "" }
    ).toArray
  }
*/
  def generateParDF(idTitleTextDB: DataFrame, source: Array[SimpleTuple], spark: SparkSession) = {

    /* TEMPO DI ESECUZIONE di questa FUNZIONE

      {"id":"ParDF","text":"Elapsed time: 259ms"}
      {"id":"ParDF","text":"Elapsed time: 0sec"}
     */
    val ids = source.map(el => el.idOfTheDoc.toString)
    idTitleTextDB.filter(col("id").isin(ids: _*))
  }


  def block_of_code(tokenizedPreprocessedDB: DataFrame, idTitleTextDB: DataFrame, myQuery: Array[String], sc: SparkContext, sparkSession: SparkSession): Unit = {

    val okapi = new ParOkapiBM25(tokenizedPreprocessedDB, myQuery, tokenizedPreprocessedDB.count())

    val scores = okapi.getBM25()
    Sorting.parMergeSort(scores, 2);

    val n = 10
    lazy val firstN: Array[SimpleTuple] = scores.take(n);

    lazy val result = generateParDF(idTitleTextDB, firstN, sparkSession)

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

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val sc = spark.sqlContext.sparkContext

    var tokenizedPreprocessedDB: DataFrame = null
    var idTitleTextDB: DataFrame = null

    //SPLIT ARGS
    val splittedArgs = args.splitAt(2)

    /* JSON TO DF  */
    splittedArgs._1(1) match {
      case "false" => {
        // READ PREPROCESSED DB
        tokenizedPreprocessedDB = readPreprocessedDBFromJson(pathWikiClean, spark)
        idTitleTextDB = readPreprocessedDBFromJson(pathIdTitleTextDB, spark)
      }
      case "true" => {
        // PREPROCESS DB and WRITE IT on S3
        val fullDB = readFullDBFromJson(path + "wikidb.json", spark)
        tokenizedPreprocessedDB = fullDB(0)
        idTitleTextDB = fullDB(1)

        //PREPROCESS FULL DB
        val preProcessData = new DataPreProcessing(tokenizedPreprocessedDB)
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

      case _ => throw new IllegalArgumentException("no boolean param has been used for -preprocess command")
    }


    val userInput = splittedArgs._2.reduce((x, y) => x + " " + y)

    val query = Seq((0, userInput))
    val queryDF = spark.createDataFrame(query).toDF("id", "__text")
    val dbQuery = new DataPreProcessing(queryDF).preProcessDF()
    val queryKeyword = dbQuery.select("words_clean").collect()
    val myQuery = queryKeyword(0).get(0).asInstanceOf[mutable.WrappedArray[String]].toArray[String]

    time(block_of_code(tokenizedPreprocessedDB, idTitleTextDB, myQuery, sc, spark), spark, "TotalExec")

  }


  def time[R](block: => R, spark: SparkSession, name: String): R = {

    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()

    val testSenteces3 = Seq(
      (name, "Elapsed time: " + (t1 - t0) / 1000000 + "ms"),
      (name, "Elapsed time: " + (t1 - t0) / 1000000000 + "sec"),
    )

    val sentence2DataFrame = spark.createDataFrame(testSenteces3).toDF("id", "text")

    sentence2DataFrame.coalesce(1).write
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
  override def toString: String = this.idOfTheDoc + ",\t" + this.value;

  def compareTo(x: SimpleTuple) = this.value - x.value
}
