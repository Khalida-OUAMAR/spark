package hapiness
import org.apache.spark.sql.SparkSession


object HappinessData extends App {
  val spark = SparkSession
    .builder()
    .appName("HappinessData")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val loadObject = new LoadDataset(spark, "src/Data")
  val loadedDf = loadObject.loadAll()


  val dataframeProcessor = new ProcessData(loadedDf, spark)

  val gov = dataframeProcessor.maxDf("Gov")
  print(gov)
  // val family = dataframeProcessor.statistics("Family")
  // val economy = dataframeProcessor.statistics("Economy")
  // val score = dataframeProcessor.statistics("Score")



}
