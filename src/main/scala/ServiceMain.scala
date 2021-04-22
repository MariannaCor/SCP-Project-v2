import com.amazonaws.{AmazonServiceException, SdkClientException}
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{ListObjectsRequest, PutObjectRequest}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.parallel.mutable.ParArray

//DONE: pre processing db con flag.
//TODO: persist su parOkapi. (S&M)
//TODO: sync su parOkapi. (S&M)
//TODO: data pre processing versione RDD.(M)
//DONE: rinominare il file programmaticamente.
//DONE: 1. sorting decrescente e take n 2.(DONE) output finale con titolo e testo dei primi n file trovati. (S) (DONE)
//DONE: lettura del DB da file s3.


object ServiceMain {

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

  def preProcessOfData(spark: SparkSession) = {

    val testSenteces3 = Seq(
      // (0, "Logistic: regression, models, are, neat."),
      (1, "Hi I heard about Spark and I think Spark is beautiful"),
      (2, "Java is Java, Spark is Spark and NoSQL is not SQL"),
      (3, "Logistic: regression, models, are, neat, spark.")
    )

    val sentence2DataFrame = spark.createDataFrame(testSenteces3).toDF("id", "text")
    val preProcessing2DB = new DataPreProcessing(sentence2DataFrame)
    preProcessing2DB.preProcessDF()

  }


  /*
  *
  *   A Dataset is a distributed collection of data.
  *   Dataset in an interface that provides the benefits of RDDs
  *   (strong typing, ability to use powerful lambda functions)
  *   with the benefits of Spark SQL’s optimized execution engine.
  *   quindi ogni volta che creiamo un dataframe è un RDD.
  *
  * */

  def generateOutput(source: Array[SimpleTuple] ,sparkSession: SparkSession): Array[(Int, Double, String, String)] = {

    val titles = readPreprocessedDBFromJson( "s3://scpmarysal/preProcessedDB/cleanTitle.json", sparkSession).collect().par;
    val texts = readPreprocessedDBFromJson( " s3://scpmarysal/preProcessedDB/wikiclean.json", sparkSession).collect();

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


  def block_of_code(preProcessedDB: DataFrame, myQuery: Array[String], sc: SparkContext, sparkSession: SparkSession): Unit = {

    val okapi = new ParOkapiBM25(preProcessedDB,myQuery, preProcessedDB.count() )

    val scores =  okapi.getBM25()
    Sorting.parMergeSort( scores,2);
    //var path = "s3://sal1/result"
    var n = 50
    lazy val firstN: Array[SimpleTuple] = scores.take(n);
    lazy val result: Array[(Int,Double,String,String)] = generateOutput( firstN, sparkSession )

    sc.parallelize(result).coalesce(1).saveAsTextFile("s3://scpmarysal/eccoIl30/")

  }


  def readFullDBFromJson(path : String , spark : SparkSession) : Array[DataFrame] = {

    val dfDB = spark.read
      .option("multiline", "true")
      .json(path)

    val arrDB = dfDB.selectExpr("mediawiki.page")

    val arrDF = new Array[DataFrame](2)

    val tmpDF = arrDB.withColumn("page", explode(col("page")))
    arrDF(0) = tmpDF.selectExpr("page.id", "page.revision.text.__text")
    arrDF(1) = tmpDF.selectExpr("page.id", "page.title")

    arrDF
  }

  def readPreprocessedDBFromJson(path : String , spark : SparkSession) : DataFrame = {
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
    val conf = new SparkConf ().setAppName ( this.getClass.getName )
    val spark: SparkSession = SparkSession.builder.config ( conf ).getOrCreate ()
    spark.sparkContext.setLogLevel ( "WARN" )
    val sc = spark.sqlContext.sparkContext

    var wikiDF : DataFrame = null

    /* id, text*/
    var wikiText : DataFrame = null

    //SPLIT ARGS
    val splittedArgs = args.splitAt(2)

    /* JSON TO DF  */
    splittedArgs._1(1) match {
      case "false" => {
        // READ PREPROCESSED DB
        wikiDF = readPreprocessedDBFromJson("s3://scpmarysal/wikiclean.json", spark)
      }
      case "true" => {
        //val path = "s3://scpmarysal/"
        val path = "C:\\Users\\Salvo\\GitHub\\SCP-Project-v2\\"
        val tempDF = readFullDBFromJson( path+"wikidb.json", spark)
        wikiDF = tempDF(0);
        val metaInfDF = tempDF(1)

        //PREPROCESS FULL DB
        val preProcessData = new DataPreProcessing(wikiDF)
        val preProcessedDB = preProcessData.preProcessDF()

        metaInfDF.write.mode("overwrite").format("json").save(path+"metaInfDB")

        //WRITE PREPROCESSED DB TO A FILE
        preProcessedDB.write
          .mode("overwrite")
          .format("json")
          .save("s3://scpmarysal/preProcessedDB")

        //RENAME DB FILE
        renameDBFile()
      }

      case _ => throw new IllegalArgumentException("no boolean param has been used for -preprocess command")
    }

    val userInput = splittedArgs._2.reduce((x,y) => x + " " + y)

    val query = Seq( (0, userInput) )
    val queryDF = spark.createDataFrame( query ).toDF( "id", "__text" )
    val dbQuery = new DataPreProcessing( queryDF ).preProcessDF()
    val queryKeyword = dbQuery.select("words_clean").collect()
    val myQuery = queryKeyword(0).get(0).asInstanceOf[mutable.WrappedArray[String]].toArray[String]

    time ( block_of_code(wikiDF,myQuery,sc,spark) , spark )

  }


  def time[R](block: => R, spark : SparkSession): R = {

    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()

    val testSenteces3 = Seq(
      (1, "Elapsed time: " + (t1 - t0)/1000000 + "ms"),
      (2, "Elapsed time: " + (t1 - t0)/1000000000 + "sec"),
    )

    val sentence2DataFrame = spark.createDataFrame(testSenteces3).toDF("id", "text")

    sentence2DataFrame.coalesce(1).write
      .mode("overwrite")
      .format("json")
      .save("s3://scpmarysal/timeOutput/")

    result
  }


  def renameDBFile() : Unit = {

    val clientRegion = "us-east-1"
    val bucketName = "scpmarysal"

    val s3= AmazonS3ClientBuilder.standard.withRegion(clientRegion).build

    val x = s3.listObjectsV2(bucketName, "preProcessedDB/")
    val y = x.getObjectSummaries()
    y.forEach( r => {
      if (r.getKey.contains("part-"))
        s3.copyObject(bucketName, r.getKey,bucketName, "wikiclean.json")
        s3.deleteObject(bucketName,  r.getKey)
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

case class SimpleTuple(idOfTheDoc: Long , value: Double){
  override def toString: String = this.idOfTheDoc+",\t"+this.value ;
  def compareTo(x: SimpleTuple) = this.value-x.value
}
