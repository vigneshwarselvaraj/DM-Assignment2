import org.apache.log4j.{Level, Logger}

object ModelBasedCF {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local[2]")
      .appName("Assignment1")
      .getOrCreate

    val inputDF = spark.read
      .format("csv")
      .option("header", "true")
      .load("test_review.csv")

    inputDF.show(5)

  }
}
