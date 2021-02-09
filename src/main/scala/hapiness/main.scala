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


  dataframeProcessor.displayDf(dataframeProcessor.maxDfYear, "max")
  dataframeProcessor.displayDf(dataframeProcessor.minDfYear, "min")
  dataframeProcessor.displayDf(dataframeProcessor.avgDfYear, "avg")


  val index = Array(0,1,2)
  for (i <- index) yield {
    val res_score = dataframeProcessor.displayLine(loadedDf,"Score", i, "v_score_"+i.toString)
    val res_eco = dataframeProcessor.displayLine(loadedDf,"Economy", i, "v_eco_"+i.toString)
    val res_family = dataframeProcessor.displayLine(loadedDf,"Family", i, "v_family_"+i.toString)
    val res_gov = dataframeProcessor.displayLine(loadedDf,"Gov", i, "v_gov_"+i.toString)
  }
}
