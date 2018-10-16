import org.apache.spark.{SparkConf, SparkContext}

/**
  * Purpose of the class. What does it do?
  * Date: 8/7/18
  *
  * @author William Lovo
  */
object SparkSessionTest extends App {
  println("Hello World")

  val conf = new SparkConf().setMaster("local").setAppName("SparkScala")
  val sc = new SparkContext(conf)
  val lines = sc.textFile("./test.txt")

  println(lines.count)
  println(lines.first)

  val words = lines.flatMap(_.split(" "))
  words.saveAsTextFile("words")

  val counts = words.map((_, 1)).reduceByKey{case (x,y)=> x+y}
  counts.saveAsTextFile("output")

  val ipsumLines = words.filter(_.contains("ipsum"))
  ipsumLines.saveAsTextFile("ipsum")

  sc.stop()
}
