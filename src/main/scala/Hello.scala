import Sorting.msort
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.StdIn


object Main {

  /*
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


  def block_of_code(preProcessedDB: DataFrame, myQuery: Array[String]): List[SimpleTuple] = {
    //val okapi = new OkapiBM25(preProcessedDB,myQuery, preProcessedDB.count(), spark )
    val okapi = new ParOkapiBM25(preProcessedDB, myQuery, preProcessedDB.count())
    val scores = okapi.getBM25()

    scores
    /*
    println("ordered list of documents with score ")
    msort(scores) map( i => println( "- "+i) )
*/
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("NetworkWordCount")
    val sc = new SparkContext(conf);
    val spark = SparkSession.builder.appName("NetworkWordCount").getOrCreate()
    val testSenteces3 = Seq(
      // (0, "Logistic: regression, models, are, neat."),
      (1, "Hi I heard about Spark and I think Spark is beautiful"),
      (2, "Java is Java, Spark is Spark and NoSQL is not SQL"),
      (3, "Logistic: regression, models, are, neat, spark.") //java spark models
    )
    // val myRdd = sc.parallelize(testSenteces3)

    //val preProcessedRDD = new DataPreProcessingRDD(myRdd, spark).preProcessRDD().map(println)


    //myRdd.foreach(println(_))
    //utente inserisce la query
    //ritorno i 10 doc più rilevanti. Conviene fare un insertion sort, man mano che scorro i documenti e poi una lettura.

    val preProcessedDB = preProcessOfData(spark)

    val userInput = args.reduce((s1, s2) => s1 + " " + s2)

    /*
      if( userInput.equals("exit") ){
        println("... ending the service"); System.exit(0); }

      println("... start calculating ")
*/

    val query = Seq((0, "Query", userInput))
    val queryDF = spark.createDataFrame(query).toDF("id", "title", "text")
    val dbQuery = new DataPreProcessing(queryDF).preProcessDF()
    var queryKeyword = dbQuery.select("words_clean").collect()
    val myQuery = queryKeyword(0).get(0).asInstanceOf[mutable.WrappedArray[String]].toArray[String]

    myQuery.map(println(_))
    val scores = block_of_code(preProcessedDB, myQuery)

    val rdd = sc.parallelize(scores)
    //rdd.foreach(println)
    rdd.coalesce(1).saveAsTextFile("s3://scpmarysal/outputTests/result-3")

    //read file from db
    //var dataframe = ReadXMLLocalFile(spark, "wikidb.xml")
  }


  def time[R](block: => R): R = {

    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    println("Elapsed time: " + (t1 - t0) / 1000000000 + "sec")

    result
  }
}
case class SimpleTuple(idOfTheDoc: Long , value: Double);
/*
*  RDD write:
*     rdd.coalesce(1).saveAsTextFile("s3://scpmarysal/outputTests/result-2")
*  DF write:
*     dfTest.write.option("header", "true").mode("overwrite").csv("s3://scpmarysal/outputTests/result-1")
*     using parquet
* */

/*
object Hello {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Test").setMaster("local[1]")
    val sc = new SparkContext(conf);
    val spark = SparkSession.builder.appName("Test").getOrCreate()

    val test = Seq(
      (0, "Logistic: regression, models, are, neat."),
      (1, "Hi I heard about Spark and I think Spark is beautiful"),
      (2, "Java is Java, Spark is Spark and NoSQL is not SQL"),
      (3, "Logistic: regression, models, are, neat, spark."),
      (4, "22222Hi I heard about Spark and I think Spark is beautiful"),
      (5, "22222Java is Java, Spark is Spark and NoSQL is not SQL"),
      (6, "22222Logistic: regression, models, are, neat, spark.")
    )

    val input = Seq((0, args.last))

    val total = test.:+(input)
    println(total)
    val rdd = sc.parallelize(total)
    rdd.coalesce(1).saveAsTextFile("s3://scpmarysal/outputTests/result-2")

    /*
    new Frame {
      title = "Hello world"

      contents = new FlowPanel {
        contents += new Label("Launch rainbows:")
        contents += new Button("Click me") {
          reactions += {
            case event.ButtonClicked(_) =>
              println("All the colours!")
          }
        }
      }

      pack()
      centerOnScreen()
      open()
    }
*/
    /*
    val dfTest = spark.createDataFrame(test).toDF("id", "text")
    val rdd = sc.parallelize(test)
    val output = rdd.collect().map(x=> x.toString).reduce( _+"\n"+_)*/
  }


}*/
