package hapiness
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{asc, avg, col, desc, expr, max, min}

class ProcessData(df: DataFrame, s: SparkSession){
  val dataframe: DataFrame = df
  val spark: SparkSession = s

  """
    |
    |""".stripMargin
  def statistics(parameter: String, df: DataFrame): DataFrame ={

    val df_Aggregates = df.agg(
      min(df(df.columns(0))).as("min"),
      avg(df(df.columns(0))).as("avg"),
      max(df(df.columns(0))).as("max"))
    df_Aggregates
  }

  def getStatistic(parameter: String, stat: String): List[Double] = {

    val df_2015 = dataframe.select(parameter + "_2015")
    val df_2016 = dataframe.select(parameter + "_2016")
    val df_2017 = dataframe.select(parameter + "_2017")

    val stats_2015 = statistics(parameter, df_2015)
    val stats_2016 = statistics(parameter, df_2016)
    val stats_2017 = statistics(parameter, df_2017)

    val stat_2015 = stats_2015.select(stats_2015.col(stat)).first().getDouble(0)
    val stat_2016 = stats_2016.select(stats_2016.col(stat)).first().getDouble(0)
    val stat_2017 = stats_2017.select(stats_2017.col(stat)).first().getDouble(0)


    val all_stats =  List(stat_2015, stat_2016, stat_2017)
    all_stats
    }

  def maxDfYear(parameter: String, year: Int): Double = {
    val list_max = getStatistic(parameter, "max")
    val max = list_max(year)
    max
  }
  def minDfYear(parameter: String, year: Int): Double = {
    val list_min = getStatistic(parameter, "min")
    val min = list_min(year)
    min
  }
  def avgDfYear(parameter: String, year: Int): Double = {
    val list_avg = getStatistic(parameter, "avg")
    val avg = list_avg(year)
    avg
  }


  def getLine(parameter:String, f: (String, Int) => Double) = {
    val year_2015 = f(parameter, 0).toString
    val year_2016 = f(parameter, 1).toString
    val year_2017 = f(parameter, 2).toString
    (parameter, year_2015, year_2016, year_2017)
  }

  def displayDf(f: (String, Int) => Double, param: String)  = {
    val gov = getLine("Gov", f)
    val eco = getLine("Economy", f)
    val family = getLine("Family", f)
    val score = getLine("Score", f)

    val my_list = Seq(gov, eco, family, score)
    val minDf = spark.createDataFrame(my_list).toDF("Parameter",
                param + "_2015",
                param + "_2016",
                param + "_2017")
    minDf.show()
  }

  def displayLine(df: DataFrame, parameter: String, year: Int, view_name: String): Unit = {

    val df_temp_table = df.createTempView(view_name)

    val new_param_name = parameter + "_201" + (year + 5).toString
    val max_value = maxDfYear(parameter, year)
    val str_number =  "_201" + (year + 5).toString
    val data = spark.sql("SELECT Country,Score"+ str_number + ",Gov"+str_number+",Economy"+str_number+",Family"+str_number
      +" from "+view_name+" where "+view_name+"."
      + new_param_name + "="
      + max_value
      + " ORDER BY "+new_param_name)
    data.show()
    data
  }



}
