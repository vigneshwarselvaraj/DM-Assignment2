import java.io.{File, PrintWriter}
import org.apache.log4j.{Level, Logger}
import scala.collection.Map
import scala.collection.immutable.Set
import scala.math.abs

object UserBased_New {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val start_time = System.currentTimeMillis()
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local[2]")
      .appName("Assignment2")
      .getOrCreate

    val sc = spark.sparkContext

    case class InitRating(userID: String, businessID: String, rating: Double)
    case class convertedRating(userID: Int, businessID: Int, rating: Double)

    val trainData = sc.textFile("train_review.csv").filter(row => !row.contains("user_id"))
    val header_line = trainData.first()
    val processedTrain = trainData.filter(line => line != header_line).map(_.split(',') match { case Array(user, business, rating) =>
      (user, business, rating.toDouble)
    })

    val testData = sc.textFile("test_review.csv")
    val test_header = testData.first()
    val processedTest = testData.filter(line => line != test_header).map(_.split(',') match { case Array(user, business, rating) =>
       (user, business, rating.toDouble)})

    val testRatings = processedTest.map{case (user, business, rating) => ((user, business), rating)}

    //testRatings.take(5).foreach(println)

    val testUserBusiness = processedTest.map{case (user, business, rating) => (user, business)}.collect().toList
    //testUserBusiness.take(10).foreach(println)

    val testTrainMerged = processedTrain ++ processedTest

    /*val userIdToInt: RDD[(String, Long)] = testTrainMerged.map(_.userID).distinct().zipWithUniqueId()
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
      intToBusinessId.collect().toMap.mapValues(_.toString).map(identity)*/


    val userBusinessMap = processedTrain.map{
      case (user, business, rating) =>
        (user, business)
    }.groupByKey().sortByKey().persist().map{ case (user, business) => (user, business.toSet)}.collectAsMap()

    val businessUserMap = processedTrain.map{
      case(user, business, rating) =>
        (business, user)
    }.groupByKey().map{case (business, user) => (business, user.toSet)}.collectAsMap()

    val userBusinessRatingMap = processedTrain.map{
      case (user, business, rating) =>
        ((user, business), rating)
    }.sortByKey().persist().collectAsMap()

    var weightSet: Map[String, Map[String, Double]] = Map()
    //var coratedAverage: Map[(String, String), Double] = Map()

    var count = 0

    for((user1, business1) <- userBusinessMap){
      var correlatedUsers: Map[String, Double] = Map()
      for((user2, business2) <- userBusinessMap){
        if(user1 < user2){
          count += 1
          val intersectingBusiness = business1.intersect(business2)

          if(intersectingBusiness.nonEmpty){
            val pearsonsCorrelation = calculatePearsonWeight(user1, user2, intersectingBusiness, userBusinessRatingMap)
            /*coratedAverage += ((user1, user2) -> avg1)
            coratedAverage += ((user2, user1) -> avg2)*/
            if(pearsonsCorrelation != 0){
              correlatedUsers += (user2 -> pearsonsCorrelation)
            }
          }
        }
        if(correlatedUsers.nonEmpty){
          weightSet += (user1 -> correlatedUsers)
        }
      }
    }

    val averageRatingMap = processedTrain.map{case (user, business, rating) => (user, (rating, 1))}
                            .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)).mapValues(y => y._1/y._2).collectAsMap()

    var predictionsMap: Map[(String, String), Double] = Map()

    for((user, business) <- testUserBusiness){
      var prediction = predict(userBusinessMap, user, business, weightSet, averageRatingMap, userBusinessRatingMap, businessUserMap)
      if(prediction > 5)
        prediction = 5
      else if(prediction < 0)
        prediction = 0
      predictionsMap += ((user, business) -> prediction)
    }

    var predictedRatings = sc.parallelize(predictionsMap.toList)
    val outputFileName = "src/main/Vigneshwar_UserBasedCF.txt"

    val printWriter = new PrintWriter(new File(outputFileName))
    val outputList = predictedRatings.map(item => {
      (item._1._1, item._1._2, item._2)
    }).sortBy(_._2).sortBy(_._1).collect()

    for (line <- outputList) {
      printWriter.write(line._1+","+ line._2 +","+ line._3+"\n")
    }
    printWriter.close()
    predictedRatings.take(5).foreach(println)

    val absError = predictedRatings.join(testRatings).map { case ((user, business), (r1, r2)) =>
      math.abs(r1 - r2)
    }

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
    //var range6 = rateDiff.filter { case (diff) => diff > 5 || diff < 0 }.count()

    val MSE = predictedRatings.join(testRatings).map { case ((user, product), (r1, r2)) => {
      val err = r1 - r2
      err * err
    }
    }.mean()

    val RMSE = math.sqrt(MSE)
    println("RMSE = " + RMSE)


    println(s"Count is $count")
    val end_time = System.currentTimeMillis()
    println("Time: " + (end_time - start_time) / 1000 + " secs")
  }

  def calculatePearsonWeight(user1: String, user2: String, intersectingBusinesses: Set[String], userBusinessRatingMap: scala.collection.Map[(String, String), Double]): Double = {
    var numerator = 0.0
    var denominator1 = 0.0
    var denominator2 = 0.0
    var user1_avg = 0.0
    var user2_avg = 0.0
    var user1RatingSum = 0.0
    var user2RatingSum = 0.0

    for (business <- intersectingBusinesses) {
      user1RatingSum += userBusinessRatingMap(user1, business)
      user2RatingSum += userBusinessRatingMap(user2, business)
    }

    user1_avg = user1RatingSum / intersectingBusinesses.size
    user2_avg = user2RatingSum / intersectingBusinesses.size

    for (item <- intersectingBusinesses) {
      numerator += (userBusinessRatingMap(user1, item) - user1_avg) * (userBusinessRatingMap(user2, item) - user2_avg)
      denominator1 += math.pow(userBusinessRatingMap(user1, item) - user1_avg, 2)
      denominator2 += math.pow(userBusinessRatingMap(user2, item) - user2_avg, 2)
    }

    val denominator = math.pow(denominator1, 0.5) * math.pow(denominator2, 0.5)
    var weight: Double = 0.0
    if (denominator != 0) {
      weight = numerator / denominator
    }
    weight
  }

  def predict(userBusinessMap: Map[String, Set[String]], user: String, business: String, weightSet: Map[String, Map[String, Double]],
              avgRating: Map[String, Double], userRatingMap: Map[(String, String), Double],
              businessUserMap: Map[String, Set[String]]): Double = {

    var numerator: Double = 0.0
    var denominator: Double = 0.0
    var u1Avg: Double = 0.0

    if(avgRating.contains(user)){
      u1Avg = avgRating(user)
    }

    var pav: Double = u1Avg
    try {
      val users: Set[String] = businessUserMap(business)
      // users that rate this product
      for (ru <- users) {
        //if(ru != user){
          if(weightSet.contains(user)) {
            if (weightSet(user).contains(ru)) {
              numerator += (userRatingMap((ru, business)) - avgRating(ru)) * weightSet(user)(ru)
              denominator += abs(weightSet(user)(ru))
              /*numerator += (userRatingMap(ru, business) - coratedAverage(ru, user)) * weightSet(user)(ru)
              denominator += abs(weightSet(user)(ru))*/
            }
          }
          else {
            if (weightSet.contains(ru)) {
              if (weightSet(ru).contains(user)) {
                numerator += (userRatingMap((ru, business)) - avgRating(ru)) * weightSet(ru)(user)
                //numerator += userRatingMap(ru, business) - coratedAverage(user, ru)
                denominator += abs(weightSet(user)(ru))
              }
            }
          }
        //}
      }

      if (denominator != 0) {
        pav = u1Avg + numerator / denominator
      }
      else {
        pav = u1Avg
      }
    }catch {
      case e : NoSuchElementException => {

      }
    }
    pav
  }

  def sum(xs: List[Double]): Double = {
    xs match {
      case x :: tail => x + sum(tail)
      case Nil => 0
    }
  }
}
