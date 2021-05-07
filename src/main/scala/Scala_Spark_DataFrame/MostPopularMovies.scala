package Scala_Spark_DataFrame

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, IntegerType, StructType}

object MostPopularMovies {

  // Define schema for dataset
  case class Movie(movieID:Int)

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("MostPopularMovies")
      .master("local[*]")
      .getOrCreate()

    val schema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    import spark.implicits._
    val df = spark.read
      .option("header", "false")
      .option("sep","\t")
      .schema(schema)
      .csv("data/ml-100k/u.data")
      .as[Movie]

    val df_new = df.groupBy("movieID").count().sort(desc("count"))

    df_new.show()

    spark.stop()

  }

}
