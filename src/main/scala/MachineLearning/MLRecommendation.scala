package MachineLearning

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}

import scala.util.Random

/**
  * Purpose of the class. What does it do?
  * Date: 8/8/18
  *
  * @author William Lovo
  */
object MLRecommendation {
  val output = ""

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "ML Recommendation App")

    val raw_data = sc.textFile(output).map(_.split("\t").take(3))
    val ratings = raw_data.map { case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble) }
    val movie_data = sc.textFile(output).map(_.split("\\|").take(2)).map(array => (array(0).toInt, array(1)))

    val model = ALS.train(ratings, 40, 15, .03)

    val users_products = ratings.map { case Rating(user, product, _) => (user, product) }
    //    val predictions = model.predict(users_products)

    val num_users = raw_data.map { case Array(user, _, _) => user }.distinct.count
    val random_user = Random.nextInt(num_users.toInt)

    val movies_from_users = ratings.keyBy(_.user).lookup(random_user)
    val top_5_fav_movies = movies_from_users.sortBy(-_.rating).take(5).map(rating =>
      (movie_data(rating.product), rating.rating))
    val top_5_rec_movies = model.recommendProducts(random_user, 5).map(rating =>
      (movie_data(rating.product), rating.rating))

    val fav_string = top_5_fav_movies.map(movie_rating =>
      f"${movie_rating._1} (Which you gave a ${movie_rating._2}%.3f point rating") mkString "\r\n"
    val rec_string = top_5_rec_movies.map("-" + _._1) mkString "\r\n"

    val formatted_string =
      f"""Your favorites:
         |Based on your ${movies_from_users.size} ratings, we found the following movies to be your favorites.
         |$fav_string
         |Your recommendations:
         |We recommend you watch the following movies, based on your ratings of other movies.
         |$rec_string
       """.stripMargin

    println(formatted_string)
  }
}
