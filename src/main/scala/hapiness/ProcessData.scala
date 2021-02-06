package hapiness
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{avg, col, max, min, typedLit}

class ProcessData(df: DataFrame, s: SparkSession){
  val dataframe: DataFrame = df
  val spark: SparkSession = s
  val index = Array(0,1,2)

  def statistics(parameter: String): Unit ={

    val df = dataframe.select(parameter + "_2015",
                                parameter + "_2016",
                                parameter + "_2017")


    var df_Stat = spark.emptyDataFrame
    
    for (i <- index) yield {
      val df_Aggregates = df.agg(
        max(df(df.columns(i))),
        min(df(df.columns(i))),
        avg(df(df.columns(i))))

      println( "max_"+ parameter+ "_201" + (5 + i).toString)
      println(df_Aggregates.columns(0))
      df_Aggregates.show()
      val df_Stats = df_Aggregates.select(df_Aggregates.columns(0),
                                        df_Aggregates.columns(1),
                                        df_Aggregates.columns(2))
        .withColumnRenamed(df_Aggregates.columns(0), "max_"+ parameter+ "_201" + (5 + i).toString)
        .withColumnRenamed(df_Aggregates.columns(1), "min_"+ parameter+ "_201" + (5 + i).toString)
        .withColumnRenamed(df_Aggregates.columns(2), "avg_"+ parameter+ "_201" + (5 + i).toString)


      import org.apache.spark.sql.functions._

      df_Stat = df_Stats.selectExpr("max_"+ parameter+ "_201" + (5 + i).toString)
      df_Stat = df_Stats.selectExpr("min_"+ parameter+ "_201" + (5 + i).toString)
      df_Stat = df_Stats.selectExpr("avg_"+ parameter+ "_201" + (5 + i).toString)
      /*
      df_Stats.withColumn("max_" + parameter + "_201" + (5 + i).toString,
        typedLit(df_Aggregates.select(df_Aggregates.columns(0)).first().getString(0)))
      df_Stats.withColumn("min_" + parameter + "_201" + (5 + i).toString,
        typedLit(df_Aggregates.select(df_Aggregates.columns(0)).first().getString(0)))
      df_Stats.withColumn("avg_" + parameter + "_201" + (5 + i).toString,
        typedLit(df_Aggregates.select(df_Aggregates.columns(0)).first().getString(0)))

      df_Aggregates.show()
       */
    }
    df_Stat.show()

  }
}