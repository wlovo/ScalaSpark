package MetricsEngine

import org.apache.spark.sql.SparkSession

/**
  * Purpose of the class. What does it do?
  * Date: 8/7/18
  *
  * @author William Lovo
  */
object BatchDriver extends App with SparkMetricsEngine {
  val location = ""

  val session = SparkSession.builder.appName("Batch Driver")/*config("spark.master", "local[*]"*/.getOrCreate
  session.sparkContext.setLogLevel("WARN")

  val raw_data = session.read.textFile(location).toDF
  val times = hourlyWindowedDF(raw_data)

  val results = allMetrics(times)

  times.show(24)

  session.stop
}
