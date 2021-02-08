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


  val max_gov = dataframeProcessor.maxDf("Gov")
  val min_gov = dataframeProcessor.minDf("Gov")
  val avg_gov = dataframeProcessor.avgDf("Gov")

  val max_score = dataframeProcessor.maxDf("Score")
  val min_score = dataframeProcessor.minDf("Score")
  val avg_score = dataframeProcessor.avgDf("Score")

  val max_eco = dataframeProcessor.maxDf("Economy")
  val min_eco = dataframeProcessor.minDf("Economy")
  val avg_eco = dataframeProcessor.avgDf("Economy")

  val max_family = dataframeProcessor.maxDf("Family")
  val min_family = dataframeProcessor.minDf("Family")
  val avg_family = dataframeProcessor.avgDf("Family")


  dataframeProcessor.displayDf(dataframeProcessor.maxDfYear, "max")
  dataframeProcessor.displayDf(dataframeProcessor.minDfYear, "min")
  dataframeProcessor.displayDf(dataframeProcessor.avgDfYear, "avg")


}
