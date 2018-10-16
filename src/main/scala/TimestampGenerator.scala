import scala.reflect.io.File
import scala.util.Random

/**
  * Purpose of the class. What does it do?
  * Date: 8/7/18
  *
  * @author William Lovo
  */
object TimestampGenerator {
  def main(args: Array[String]): Unit = {
    val num_files = 50
    val max_iterations = 1000
    val min_iterations = 500

    val gen_log_data = StringBuilder.newBuilder
    val output = ""

    val rand = Random

    (0 to num_files).foreach { curr =>
      (0 to rand.nextInt(max_iterations - min_iterations) + min_iterations).foreach { iter =>
        gen_log_data.append(f"{datetime=${rand.nextInt(24)}%02d:${rand.nextInt(60)}%02d:${rand.nextInt(60)}%02d.${rand.nextInt(9999)}%04d}\n")
      }
      File(f"${output}gen_log$curr%02d.txt").createFile(false).writeAll(gen_log_data.toString)
      gen_log_data.clear
    }
  }
}
