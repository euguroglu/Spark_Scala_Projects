package Scala_Spark_RDD

import org.apache.log4j._
import org.apache.spark._

object CustomerAmount {

  def parseLine(line:String): (Int, Float) = {
    val fields = line.split(",")
    val customerID = fields(0).toInt
    val amount = fields(2).toFloat
    (customerID, amount)
  }

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "Customer_Amount")

    // Load data as RDD
    val lines = sc.textFile("data/customer-orders.csv")

    // Convert data to customerID, amount tuple
    val parsedLines = lines.map(parseLine)

    val totalAmount = parsedLines.reduceByKey((x,y) => x+y)

    val flippedAmount = totalAmount.map((x => (x._2, x._1)))

    val sortedAmount = flippedAmount.sortByKey()

    val results = sortedAmount.collect()

    //results.foreach(println)

    for (result <- results) {
      val customer = result._2
      val amount_spend = result._1
      println(f"Customer $customer spend $$$amount_spend%.1f ")
    }

  }
}
