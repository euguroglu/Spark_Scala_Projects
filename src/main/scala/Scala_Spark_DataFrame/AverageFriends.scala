package Scala_Spark_DataFrame


import org.apache.log4j._
import org.apache.spark.sql._

object AverageFriends {

  case class Person(id:Int, name:String, age:Int, friends:Int)

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL_Friends")
      .master("local[*]")
      .getOrCreate()

    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    println("Here is our inferred schema:")
    people.printSchema()

    val df = people.select("age", "friends").groupBy("age").avg("friends")

    df.show()

    val df_sorted = df.sort("age")

    df_sorted.show()

    val df_formatted = people.select("age","friends")
                             .groupBy("age")
                             .agg(functions.round(functions.avg("friends"), 2))
                             .sort("age")

    df_formatted.show()

    val df_columnnamed = people.select("age", "friends")
                                .groupBy("age")
                                .agg(functions.round(functions.avg("friends"),2)).alias("avg_friends")
                                .sort("age")

    df_columnnamed.show()





  }

}
