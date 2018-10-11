import org.apache.hadoop.util.hash.Hash
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.recommendation
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.hash


object ModelBasedCF {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local[2]")
      .appName("Assignment1")
      .getOrCreate

    val sc = spark.sparkContext

    val inputDF = spark.read
      .format("csv")
      .option("header", "true")
      .load("test_review.csv")

    inputDF.show(5)

    val inputData = sc.textFile("test_review.csv")
    val header_line = inputData.first()

    /*val processedInput = inputData.filter(line => line != header_line)
                            .map(line => line.split(","))
                            .map(words => (words(0).toInt, words(1).toInt, words(2).toDouble))*/

    /*val ratings = inputData.filter(line => line != header_line).map(_.split(',') match { case Array(user, business, rate) =>
                              Rating(user.toInt, business.toInt, rate.toDouble)
                          })*/


    case class InitRating(userID: String, businessID: String, rating: Double)
    case class convertedRating(userID: Int, businessID: Int, rating: Double)

    val processedInput = inputData.filter(line => line != header_line).map(_.split(',') match { case Array(user, business, rate) =>
    InitRating(user, business, rate.toDouble)})

    val userIdToInt: RDD[(String, Long)] = processedInput.map(_.userID).distinct().zipWithUniqueId()
    val intToUserID: RDD[(Long, String)] =  userIdToInt.map { case (l, r) => (r, l) }

    val businessIdToInt: RDD[(String, Long)] = processedInput.map(_.businessID).distinct().zipWithUniqueId()
    val intToBusinessId: RDD[(Long, String)] = businessIdToInt.map { case (l, r) => (r, l)}


    val mapID: Map[String, Int] =
      userIdToInt.collect().toMap.mapValues(_.toInt).map(identity)

    mapID.take(5).foreach(println)

    val intToUserMap: Map[Long, String] =
      intToUserID.collect().toMap.mapValues(_.toString).map(identity)

    val mapBusiness: Map[String, Int] =
      businessIdToInt.collect().toMap.mapValues(_.toInt).map(identity)

    val intToBusinessMap: Map[Long, String] =
      intToBusinessId.collect().toMap.mapValues(_.toString).map(identity)

    mapBusiness.take(5).foreach(println)

    val ratings = processedInput.map{ r =>
      Rating(mapID(r.userID), mapBusiness(r.businessID), r.rating)
    }

    /*val ratings = processedInput2.map{ r =>
        Rating(r.userID, r.businessID, r.rating)
    }*/

    ratings.take(5).foreach(println)

    //val mapRatings = processedInput.map(line => Rating(line(0)(0), line(0)(1), line(1)))
    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    /*val usersBusinesses = ratings.map{ case Rating(user, business, rate) => (user, business)}
    val predictions = model.predict(usersBusinesses).map{ case Rating(user, business, rate) => ((user, business), rate)}

    val ratesAndPreds = ratings.map{ case Rating(user, business, rate) => ((user, business), rate)}.join(predictions)


    def roundUp(d: Double) = math.ceil(d).toInt


    val MSE = ratesAndPreds.map { case ((user, business), (r1, r2)) =>
              val err = (r1 - r2)
              err * err
    }.mean()

    val MSErounded = "%.2f".format(MSE)

    println(s"Mean Squared Error = $MSErounded")*/

    val testData = sc.textFile("test_review.csv")
    val test_header = testData.first()
    val processedTest = testData.filter(line => line != test_header).map(_.split(',') match { case Array(user, business, rating) =>
      InitRating(user, business, rating.toDouble) })

    val ratings2 = processedTest.map{ r =>
      Rating(mapID(r.userID), mapBusiness(r.businessID), r.rating)
    }

    val testUsersBusinesses = processedTest.map{case InitRating(userID, businessID, rating) =>
      (mapID(userID), mapBusiness(businessID))
    }

    val predictions = model.predict(testUsersBusinesses).map{ case Rating(user, business, rate) => ((user, business), rate)}
    val ratesAndPreds = ratings2.map{ case Rating(user, business, rate) => ((user, business), rate)}.join(predictions)

    val predictionsToString = predictions.map { case ((user, business), rate) => ((intToUserMap(user)), intToBusinessMap(business), rate)}

    predictionsToString.take(50).foreach(println)

    val MSE = ratesAndPreds.map { case ((user, business), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()

    //val zeroToOne = ratesAndPreds.map { case ((user, business), (r1, r2)) =>

    val absError = ratesAndPreds.map{case ((user, business), (r1, r2))  => Math.abs(r1 - r2)}
    val btw0And1 = absError.filter(err => 0 <= err && err < 1).count()
    val btw1And2 = absError.filter(err => 1 <= err && err < 2).count()
    val btw2And3 = absError.filter(err => 2 <= err && err < 3).count()
    val btw3And4 = absError.filter(err => 3 <= err && err < 4).count()
    val btw4And5 = absError.filter(err => 4 <= err && err < 5).count()

    println(s"Between 0 and 1 count is $btw0And1")
    println(s"Between 1 and 2 count is $btw1And2")
    println(s"Between 2 and 3 count is $btw2And3")
    println(s"Between 3 and 4 count is $btw3And4")
    println(s"Between 4 and 5 count is $btw4And5")

    val MSERounded = "%.2f".format(MSE)
    println(s"Mean Squared Error = $MSE")
  }
}
