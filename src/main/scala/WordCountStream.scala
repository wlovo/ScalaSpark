import org.apache.spark.sql.SparkSession

/**
  * Purpose of the class. What does it do?
  * Date: 8/8/18
  *
  * @author William Lovo
  */
object WordCountStream extends App {
  val spark = SparkSession.builder.appName("Word Count").config("spark.master", "local[*]").getOrCreate

  val read_stream_options = Map("host" -> "localhost", "port" -> "9999")

  import spark.implicits._

  val line_stream = spark.readStream.format("socket").options(read_stream_options).load

  val words = line_stream.as[String].flatMap(_.split(" "))

  val word_count = words.groupBy("values").count()

  val query = word_count.writeStream.outputMode("console").start
  query.awaitTermination
}
