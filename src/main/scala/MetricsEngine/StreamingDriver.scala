package MetricsEngine

import org.apache.spark.sql.SparkSession

/**
  * Purpose of the class. What does it do?
  * Date: 8/7/18
  *
  * @author William Lovo
  */
object StreamingDriver extends App with SparkMetricsEngine {
  val location = ""
  val stream_options = Map("numRows" -> "24", "truncate" -> "false")

  val spark = SparkSession.builder.appName("Batch Driver")/*.config("spark.master", "local[*]"*/.getOrCreate
  spark.sparkContext.setLogLevel("WARN")

  val stream = spark.readStream.option("maxFilesPerTrigger", 1).text(location)

  val times = hourlyWindowedDF(stream)
//  val results = allMetrics(times)

  val query = times.writeStream.outputMode("complete").format("console").options(stream_options).start

  query.awaitTermination
}
