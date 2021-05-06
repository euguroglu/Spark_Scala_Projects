package Scala_Spark_DataFrame

import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import scala.math.max

object MaxTemperaturesDataset {

  // Define schema for dataset
  case class Temperature(stationID:String, date:Int, measure_type:String, temperature:Float)

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL_MaxTemperature")
      .master("local[*]")
      .getOrCreate()

      val schema = new StructType()
        .add("stationID", StringType, nullable =  true)
        .add("date", IntegerType, nullable =  true)
        .add("measure_type", StringType, nullable =  true)
        .add("temperature", FloatType, nullable =  true)

    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
    val temp = spark.read
      .option("header", "false")
      .schema(schema)
      .csv("data/1800.csv")
      .as[Temperature]

    println("Here is our inferred schema:")
    temp.printSchema()

    val max_temp = temp.filter($"measure_type" === "TMAX")

    val max_select = max_temp.select("stationID", "temperature")

    val max_temp_group = max_select.groupBy("stationID").max("temperature")

    val max_temp_fahrenheit = max_temp_group
      .withColumn("temperature", round($"max(temperature)"*0.1f * (9.0f /5.0f) + 32.0f, 2))
      .select("stationID","temperature").sort("temperature")

    max_temp_fahrenheit.show()

    for (i <- max_temp_fahrenheit) {
      val station = i(0)
      val temp = i(1).asInstanceOf[Float]
      val formattedTemp = f"$temp%.2f F"
      println(s"$station maximum temperature: $formattedTemp")
    }
  }

}
