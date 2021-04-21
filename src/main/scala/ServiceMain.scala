import org.apache.spark.sql.functions.{col, explode}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


import scala.collection.mutable

//DONE: pre processing db con flag.
//TODO: persist su parOkapi. (S&M)
//TODO: sync su parOkapi. (S&M)
//TODO: data pre processing versione RDD.(M)
//TODO: rinominare il file. (S)
//TODO: 1. sorting decrescente e take n 2. output finale con titolo e testo dei primi n file trovati. (S)
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


  def block_of_code(preProcessedDB: DataFrame, myQuery: Array[String], sc: SparkContext): Unit = {
    val okapi = new ParOkapiBM25(preProcessedDB,myQuery, preProcessedDB.count() )
    val scores =  okapi.getBM25()
    //println("ordered list of documents with score ")
    Sorting.parMergeSort(scores,0);
    //var path = "s3://sal1/result"
    //parMergeSort
    //scores reverseMap(  println(_)  )

    sc.parallelize(scores).coalesce(1).saveAsTextFile("s3://scpmarysal/eccoIl30/")

  }


  def readFullDBFromJson(path : String , spark : SparkSession) : DataFrame = {
    val dfDB = spark.read
      .option("multiline", "true")
      .json(path)

    val arrDB = dfDB.selectExpr("mediawiki.page")
    arrDB.withColumn("page", explode(col("page"))).selectExpr("page.id", "page.revision.text.__text")
  }

  def readPreprocessedDBFromJson(path : String , spark : SparkSession) : DataFrame = {
    val dfDB = spark.read
      .json(path)

    dfDB
  }


  /*
  *  -preprocess false/true keywords
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
        wikiDF = readPreprocessedDBFromJson("s3://scpmarysal/preProcessedDB/wikiclean.json", spark)

      }
      case "true" => {
        wikiDF = readFullDBFromJson("s3://scpmarysal/wikidb.json", spark)

        //PREPROCESS FULL DB
        val preProcessData = new DataPreProcessing(wikiDF)
        val preProcessedDB = preProcessData.preProcessDF()

        //WRITE PREPROCESSED DB TO A FILE
        preProcessedDB.write
          .mode("overwrite")
          .format("json")
          .save("s3://scpmarysal/preProcessedDB")
      }
    }

    val userInput = splittedArgs._2.reduce((x,y) => x + " " + y)

    val query = Seq( (0, userInput) )
    val queryDF = spark.createDataFrame( query ).toDF( "id", "__text" )
    val dbQuery = new DataPreProcessing( queryDF ).preProcessDF()
    val queryKeyword = dbQuery.select("words_clean").collect()
    val myQuery = queryKeyword(0).get(0).asInstanceOf[mutable.WrappedArray[String]].toArray[String]

    time ( block_of_code(wikiDF,myQuery,sc) , spark )

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

}

case class SimpleTuple(idOfTheDoc: Long , value: Double){
  override def toString: String = this.idOfTheDoc+",\t"+this.value ;
  def compareTo(x: SimpleTuple) = this.value-x.value
}

/*
*
*
* def ReadXMLLocalFile(spark: SparkSession, xmlFile: String): DataFrame = {

 val revisionStruct = StructType(
StructField("text", StringType, true) :: Nil)

 val schema = StructType(
StructField("title", StringType, true) ::
StructField("id", IntegerType, false) ::
StructField("revision.text", StringType, false) :: Nil)

 val df = spark.sqlContext.read.format("com.databricks.spark.xml")
.option("rootTag", "mediawiki")
.option("rowTag", "page")
//.schema(schema)
.load(xmlFile)

 df.select("title", "id", "revision")

 }
*
*
* */