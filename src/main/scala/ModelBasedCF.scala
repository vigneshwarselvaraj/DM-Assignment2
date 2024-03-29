import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD

object ModelBasedCF {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local[2]")
      .appName("Assignment1")
      .getOrCreate

    val sc = spark.sparkContext

    val inputData = sc.textFile("train_review.csv")
    val header_line = inputData.first()
    case class InitRating(userID: String, businessID: String, rating: Double)
    case class convertedRating(userID: Int, businessID: Int, rating: Double)

    val processedInput = inputData.filter(line => line != header_line).map(_.split(',') match { case Array(user, business, rate) =>
    InitRating(user, business, rate.toDouble)})

    val testData = sc.textFile("test_review.csv")
    val test_header = testData.first()
    val processedTest = testData.filter(line => line != test_header).map(_.split(',') match { case Array(user, business, rating) =>
      InitRating(user, business, rating.toDouble) })

    val testTrainMerged = processedInput ++ processedTest

    val userIdToInt: RDD[(String, Long)] = testTrainMerged.map(_.userID).distinct().zipWithUniqueId()
    val intToUserID: RDD[(Long, String)] =  userIdToInt.map { case (l, r) => (r, l) }

    val businessIdToInt: RDD[(String, Long)] = testTrainMerged.map(_.businessID).distinct().zipWithUniqueId()
    val intToBusinessId: RDD[(Long, String)] = businessIdToInt.map { case (l, r) => (r, l)}

    val mapID: Map[String, Int] =
      userIdToInt.collect().toMap.mapValues(_.toInt).map(identity)
    val intToUserMap: Map[Long, String] =
      intToUserID.collect().toMap.mapValues(_.toString).map(identity)

    val mapBusiness: Map[String, Int] =
      businessIdToInt.collect().toMap.mapValues(_.toInt).map(identity)
    val intToBusinessMap: Map[Long, String] =
      intToBusinessId.collect().toMap.mapValues(_.toString).map(identity)

    val ratings = processedInput.map{ r =>
      Rating(mapID(r.userID), mapBusiness(r.businessID), r.rating)
    }

    val rank = 3
    val numIterations = 20
    val model = ALS.train(ratings, rank, numIterations, 0.3)

    val ratings2 = processedTest.map{ r =>
      Rating(mapID(r.userID), mapBusiness(r.businessID), r.rating)
    }

    val testUsersBusinesses = processedTest.map{case InitRating(userID, businessID, rating) =>
      (mapID(userID), mapBusiness(businessID))
    }

    val predictions = model.predict(testUsersBusinesses).map{ case Rating(user, business, rate) =>
      ((user, business), rate)
    }

    val ratesAndPreds = ratings2.map{ case Rating(user, business, rate) => ((user, business), rate)}.join(predictions)

    val predictionsToString = predictions.map { case ((user, business), rate) => (intToUserMap(user), intToBusinessMap(business), rate)}

    val MSE = ratesAndPreds.map { case ((user, business), (r1, r2)) =>
      val err = r1 - r2
      err * err
    }.mean()

    val absError = ratesAndPreds.map{case ((user, business), (r1, r2))  => Math.abs(r1 - r2)}
    val btw0And1 = absError.filter(err => 0 <= err && err < 1).count()
    val btw1And2 = absError.filter(err => 1 <= err && err < 2).count()
    val btw2And3 = absError.filter(err => 2 <= err && err < 3).count()
    val btw3And4 = absError.filter(err => 3 <= err && err < 4).count()
    val btw4And5 = absError.filter(err => 4 <= err && err < 5).count()

    println(s">=0 and <1: $btw0And1")
    println(s">=1 and <2: $btw1And2")
    println(s">=2 and <3: $btw2And3")
    println(s">=3 and <4: $btw3And4")
    println(s">=4 and <5: $btw4And5")

    val rms = math.sqrt(MSE)
    val rmsRounded = "%.3f".format(rms)
    println(s"Mean Squared Error = $rmsRounded")

    val finalRDD = predictionsToString.sortBy(_._2).sortBy(_._1).map{ case (user, business, rate) =>
      user + ", " + business + ", " + rate}

    finalRDD.coalesce(1)
      .saveAsTextFile("Task1Output.txt")
  }
}
