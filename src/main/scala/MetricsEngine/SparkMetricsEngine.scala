package MetricsEngine

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.joda.time.DateTime

/**
  * Purpose of the class. What does it do?
  * Date: 8/7/18
  *
  * @author William Lovo
  */
trait SparkMetricsEngine {

  def hourlyWindowedDF(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    val base = new DateTime()

    val extract_time = df.sparkSession.udf.register("extract_time", (x: String) => {
      val datetime_format = """datetime=(\d{2}):(\d{2}):(\d{2}).(\d{4})""".r.unanchored

      x match {
        case datetime_format(hour, minute, second, _) =>
          Some(s"${base.getYear}-${base.getMonthOfYear}-${base.getDayOfMonth} $hour:$minute:$second")
        case _ => None
      }
    })

    val logs = df.select(unix_timestamp(extract_time($"value")) cast TimestampType as "Timestamp").
      groupBy(window($"timestamp", "1 hour") as "Timestamp Window").count.sort($"Timestamp Window")

    logs
  }

  def dfMax(df: DataFrame): DataFrame = {
    df.selectExpr("max(count) as Max")
  }

  def dfMin(df: DataFrame): DataFrame = {
    df.selectExpr("min(count) as Min")
  }

  def dfAvg(df: DataFrame): DataFrame = {
    df.selectExpr("avg(count) as Avg")
  }

  def dfMed(df: DataFrame): DataFrame = {
    df.selectExpr("percentile_approx(count,0.5) as Median")
  }

  def dfSum(df: DataFrame): DataFrame = {
    df.selectExpr("sum(count) as Sum")
  }

  def allMetrics(df: DataFrame): DataFrame = {
    df.selectExpr("max(count) as Max", "min(count) as Min", "avg(count) as Avg",
      "percentile_approx(count,0.5) as Median", "sum(count) as Sum")
  }

}
