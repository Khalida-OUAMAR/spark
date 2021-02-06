package hapiness
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

class LoadDataset(s: SparkSession, filename_rootdir: String){
  def spark = s
  def rootdir : String = filename_rootdir

  def loadDataframe(year: Int):  DataFrame = {
    val str_year = year.toString

    val happyData = spark.read.option("inferSchema", "true")
      .option("header", "true")
      .csv(rootdir + "/" + str_year + ".csv")

    happyData.printSchema()
    val df_Res = happyData.select("Country",
                                  "Happiness Score",
                                  "Trust (Government Corruption)",
                                  "Family",
                                  "Economy (GDP per Capita)")
      .withColumnRenamed("Happiness Score", "Score_" + str_year)
      .withColumnRenamed("Trust (Government Corruption)", "Gov_" + str_year)
      .withColumnRenamed("Family", "Family_" + str_year)
      .withColumnRenamed("Economy (GDP per Capita)", "Economy_" + str_year)

    df_Res
  }

  def loadDataframe2017():  DataFrame = {
    import org.apache.spark.sql.functions.col
    val happyData2017 = spark.read.option("inferSchema","true")
                                  .option("header","true")
                                  .csv(rootdir + "/2017.csv")
    happyData2017.printSchema()

    val df_Res2017 = happyData2017.select(col("Country"),
      col("`Happiness.Score`").alias("Score_2017"),
      col("`Trust..Government.Corruption.`").alias("Gov_2017"),
      col("Family").alias("Family_2017"),
      col("`Economy..GDP.per.Capita.`").alias("Economy_2017"))

    df_Res2017
  }

  def loadAll():  DataFrame = {
    val df_Res2015 = loadDataframe(2015)
    val df_Res2016 = loadDataframe(2016)
    val df_Res2017 = loadDataframe2017()

    val finalResult = df_Res2015
      .join(df_Res2016, "Country")
      .join(df_Res2017, "Country")
    finalResult.show()
    finalResult
  }



}