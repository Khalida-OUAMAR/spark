package hapiness
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{asc, avg, col, desc, expr, max, min}

class ProcessData(df: DataFrame, s: SparkSession){
  val dataframe: DataFrame = df
  val spark: SparkSession = s
  val index = Array(0,1,2)

  def statistics(parameter: String, df: DataFrame): DataFrame ={

    println(">>>>>> df show ")
    df.show()
    val df_Aggregates = df.agg(
      min(col(parameter)).as("min"),
      avg(col(parameter)).as("avg"),
      max(col(parameter)).as("max"))
    println(">>>>>>>>>>> show df aggregates : ")
    df_Aggregates.show()
    df_Aggregates
  }

  def getStatistic(parameter: String, stat: String): Seq[Double] = {

    val df_2015 = dataframe.select(parameter + "_2015")
    val df_2016 = dataframe.select(parameter + "_2016")
    val df_2017 = dataframe.select(parameter + "_2017")

    df_2015.show()
    val stats_2015 = statistics(parameter, df_2015)
    val stats_2016 = statistics(parameter, df_2016)
    val stats_2017 = statistics(parameter, df_2017)

    val stat_2015 = stats_2015.select(stats_2015.col(stat)).first().getString(0).toDouble
    val stat_2016 = stats_2016.select(stats_2016.col(stat)).first().getString(0).toDouble
    val stat_2017 = stats_2017.select(stats_2017.col(stat)).first().getString(0).toDouble


    val all_stats =  List(stat_2015, stat_2016, stat_2017)
    all_stats
    }


  def maxDf(parameter: String): Double = {
    val list_max = getStatistic(parameter, "max")
    val max = list_max.max
    max
  }
  def minDf(parameter: String): Double = {
    val list_min = getStatistic(parameter, "min")
    val min = list_min.min
    min
  }
  def avgDf(parameter: String): Double = {
    val list_avg = getStatistic(parameter, "avg")
    val avg = list_avg.sum / list_avg.length
    avg
  }

  }
