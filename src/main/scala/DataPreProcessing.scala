import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


class DataPreProcessing( private var df: DataFrame )  {

  def preProcessDF() : DataFrame = {
    val cleanedDF = removePuntuaction()
    val tokenizedDF = tokenize(cleanedDF)
    removeStopWords(tokenizedDF)
  }

  def setDF(newDF: DataFrame) : Unit= {
    df = newDF
  }

  //return sentenceDF_clean
  private def removePuntuaction() : DataFrame = {
    df.select(col("id"), lower(regexp_replace(col("__text"), "[^a-zA-Z\\s]", "")).alias("text"))
  }

  private def tokenize(df: DataFrame) : DataFrame = {
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    tokenizer.transform(df).select(col("id"), col("words"))
  }

  private def removeStopWords(df: DataFrame) : DataFrame = {
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("words_clean")
    remover.transform(df).select(col("id"), col("words_clean"))
  }

  /* //STEMMING (not working)
  val stemmer = new Stemmer().setInputCols("words_clean").setOutputCol("words_stemmed").setLanguage("English")
  val wordsDF_stemmed = stemmer.transform(wordsDF_clean).select(col("id"), col("words_stemmed"))
  wordsDF_stemmed.show(false). */

}
