import java.lang.System

import Sorting.msort
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.mutable
import scala.io.StdIn

object ServiceMain {




  def ReadXMLLocalFile(spark: SparkSession, xmlFile: String): DataFrame = {

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


  def block_of_code(preProcessedDB: DataFrame, myQuery: Array[String], spark: SparkSession): Unit = {
    val okapi = new ParOkapiBM25(preProcessedDB,myQuery, preProcessedDB.count() )
    //val okapi = new OkapiBM25(preProcessedDB,myQuery, preProcessedDB.count(), spark )
    val scores =  okapi.getBM25()
    println("ordered list of documents with score ")
    scores.foreach(println)
    //msort(scores.toList) map( i => println( "- "+i) )

  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val sc = new SparkContext(conf);
    val spark = SparkSession.builder.appName("NetworkWordCount").getOrCreate()
    val testSenteces3 = Seq(
      // (0, "Logistic: regression, models, are, neat."),
      (1, "Hi I heard about Spark and I think Spark is beautiful"),
      (2, "Java is Java, Spark is Spark and NoSQL is not SQL"),
      (3, "Logistic: regression, models, are, neat, spark.") //java spark models
    )
    val myRdd = sc.parallelize(testSenteces3)

    myRdd.foreach(println(_))
    //utente inserisce la query
    //ritorno i 10 doc più rilevanti. Conviene fare un insertion sort, man mano che scorro i documenti e poi una lettura.

    val preProcessedDB = preProcessOfData(spark)

      //PROCESS QUERY
      Console.out.println( "Search something or digit exit to quit:" )
      val userInput = args.reduce((x,y) => x + " " + y)

      val query = Seq( (0, "Query", userInput) )
      val queryDF = spark.createDataFrame( query ).toDF( "id", "title", "text" )
      val dbQuery = new DataPreProcessing( queryDF ).preProcessDF()
      var queryKeyword = dbQuery.select("words_clean").collect()
      val myQuery = queryKeyword(0).get(0).asInstanceOf[mutable.WrappedArray[String]].toArray[String]

      myQuery.map(println(_)  )
      time ( block_of_code(preProcessedDB,myQuery,spark) )


    //read file from db
    //var dataframe = ReadXMLLocalFile(spark, "wikidb.xml")
  }


  def time[R](block: => R): R = {

    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000 + "ms")
    println("Elapsed time: " + (t1 - t0)/1000000000 + "sec")

    result
  }

}

case class SimpleTuple(idOfTheDoc: Long , value: Double);