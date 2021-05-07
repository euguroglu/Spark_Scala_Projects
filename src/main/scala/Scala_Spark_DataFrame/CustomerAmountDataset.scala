package Scala_Spark_DataFrame

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StructType}

object CustomerAmountDataset {

  // Define schema for dataset
  case class Customer(customerid:Int, itemID:Int, amount:Float)

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("SparkSQL_Customer_Amount")
      .master("local[*]")
      .getOrCreate()

    val schema = new StructType()
      .add("customerid", IntegerType, true)
      .add("itemID", IntegerType, true)
      .add("amount", FloatType, true)


    // Convert our csv file to a DataSet, using our Customer case
    // class to infer the schema.
    import spark.implicits._
    val df = spark.read
      .option("header", "false")
      .schema(schema)
      .csv("data/customer-orders.csv")
      .as[Customer]

    println("Here is our inferred schema:")
    df.printSchema()

    val df_new = df.select("customerid", "amount")

    val df_new_2 = df_new.groupBy("customerid").agg(round(sum("amount"),2).alias("totalAmount"))

    val df_sorted =  df_new_2.sort("totalAmount")

    df_sorted.show(df_sorted.count().toInt)

  }

}
