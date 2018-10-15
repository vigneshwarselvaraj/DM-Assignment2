import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

object UserBasedCF {
  def main(args: Array[String]) : Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val start_time = System.currentTimeMillis()

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local[2]")
      .appName("Assignment1")
      .getOrCreate

    val sc = spark.sparkContext

    val trainData = sc.textFile("train_review.csv")
    val header_line = trainData.first()
    val processedTrain = trainData.filter(line => line != header_line).map(_.split(',') match { case Array(user, business, rating) =>
      (user, (business, rating.toDouble))
    })

    case class InitRating(userID: String, businessID: String, rating: Double)
    case class convertedRating(userID: Int, businessID: Int, rating: Double)

    val testData = sc.textFile("test_review.csv")
    val test_header = testData.first()
    val processedTest = testData.filter(line => line != test_header).map(_.split(',') match { case Array(user, business, rating) =>
      InitRating(user, business, rating.toDouble) })

    val processedInput = trainData.filter(line => line != header_line).map(_.split(',') match { case Array(user, business, rate) =>
      InitRating(user, business, rate.toDouble)})

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


    //processedTrain.sortBy(_._1).take(2).foreach(println)

    val combinedTrainValues = processedInput.map{case InitRating(user, business, rate) => (user, (business, rate))}
      .aggregateByKey(List[(String, Double)]())(
      (acc, x) => x::acc,
      (acc1, acc2) => acc1:::acc2
    ).sortBy(_._1)

    combinedTrainValues.take(2).foreach(println)

    println(combinedTrainValues.collect().length)

    val combinedTrainValuesList = combinedTrainValues.collect()

    /*val combinationVals = combinedTrainValues.collect().combinations(2).map{
      case Array(a, b) => (a, b)
    }

    var  count= 0.0
    for((user1, user2) <- combinationVals){
      count += 1
    }*/

    val userCombinations = combinedTrainValues.collect().combinations(2).map{
      case Array(a, b) => (a, b)}

    var coratedAverage: Map[(String, String), Double] = Map()
    var weightsOfAllUsers: Map[(String, String), Double] = Map()

    for(i <- combinedTrainValuesList.indices){
      for(j <- combinedTrainValuesList.indices){
        if(i < j){
          var user1 = combinedTrainValuesList(i)
          var user2 = combinedTrainValuesList(j)
          var user1_ratings = List[Double]()
          var user2_ratings = List[Double]()
          var foundEqualBusiness = false
          for((business1:String, rating1:Double) <- user1._2){
            for((business2:String, rating2:Double) <- user2._2){
              if(business1 == business2){
                foundEqualBusiness = true
                user1_ratings = rating1 :: user1_ratings
                user2_ratings = rating2 :: user2_ratings
              }
            }
          }

          if(user1_ratings.nonEmpty){
            coratedAverage += ((user1._1, user2._1) -> userAvg(user1._1, user1_ratings))
            coratedAverage += ((user2._1, user1._1) -> userAvg(user2._1, user2_ratings))
            weightsOfAllUsers += ((user1._1, user2._1) -> calculateWeight(user1._1, user2._1, user1_ratings, user2_ratings))
          }
          else{
            weightsOfAllUsers += ((user1._1, user2._1) -> 0)
          }
        }
      }
    }

    /*
    for((user1, user2) <- userCombinations){
      var user1_ratings = List[Double]()
      var user2_ratings = List[Double]()
      var foundEqualBusiness = false
      for((business1:String, rating1:Double) <- user1._2){
        for((business2:String, rating2:Double) <- user2._2){
          if(business1 == business2){
            foundEqualBusiness = true
            user1_ratings = rating1 :: user1_ratings
            user2_ratings = rating2 :: user2_ratings
          }
        }
      }

      if(user1_ratings.nonEmpty){
        coratedAverage += ((user1._1, user2._1) -> userAvg(user1._1, user1_ratings))
        coratedAverage += ((user2._1, user1._1) -> userAvg(user2._1, user2_ratings))
        weightsOfAllUsers += ((user1._1, user2._1) -> calculateWeight(user1._1, user2._1, user1_ratings, user2_ratings))
      }
      else{
        weightsOfAllUsers += ((user1._1, user2._1) -> 0)
      }
    }*/
    //weightsOfAllUsers.take(10).foreach(println)

    val end_time = System.currentTimeMillis()
    println("Time: " + (end_time - start_time) / 1000 + " secs")
  }

  def calculateWeight(user1: String, user2:String, ratings1: List[Double], ratings2: List[Double]): Double ={
    //val user1_average = sum(ratings1) / ratings1.length
    //val user2_average = sum(ratings2) / ratings2.length

    var user1_sum = 0.0
    var user2_sum = 0.0
    for(i <- ratings1.indices){
      user1_sum += ratings1(i)
      user2_sum += ratings2(i)
    }

    val user1_average = user1_sum / ratings1.length
    val user2_average = user2_sum / ratings2.length

    var numerator = 0.0
    var denominator1 = 0.0
    var denominator2 = 0.0
    var weight = 0.0

    for (i <- ratings1.indices){
      numerator += (ratings1(i) - user1_average) * (ratings2(i) - user2_average)
      denominator1 += Math.pow(ratings1(i) - user1_average, 2)
      denominator2 += Math.pow(ratings2(i) - user2_average, 2)
    }

    val denominator = math.sqrt(denominator1) * math.sqrt(denominator2)

    if (denominator != 0){
      weight = numerator / denominator
    }
    weight
  }

  def userAvg(user1: String, ratings1: List[Double]): Double = {
    sum(ratings1) / ratings1.length
  }

  def sum(xs: List[Double]): Double = {
    xs match {
      case x :: tail => x + sum(tail)
      case Nil => 0
    }
  }
}
