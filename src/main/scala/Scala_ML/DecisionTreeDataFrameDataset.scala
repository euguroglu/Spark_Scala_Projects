package Scala_ML

import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.sql._


object DecisionTreeDataFrameDataset {
  
  case class DecisionSchema(No: Int, TransactionDate: Double, HouseAge: Double,
                            DistanceToMRT: Double, NumberConvenienceStores: Int, Latitude: Double, Longitude: Double,
                            PriceOfUnitArea: Double)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("DecisionTreeDF")
      .master("local[*]")
      .getOrCreate()
      
    // Load up our page speed / amount spent data in the format required by MLLib
    // (which is label, vector of features)
      
    // In machine learning lingo, "label" is just the value you're trying to predict, and
    // "feature" is the data you are given to make a prediction with. So in this example
    // the "labels" are the first column of our data, and "features" are the second column.
    // You can have more than one "feature" which is why a vector is required.

    import spark.implicits._
    val dsRaw = spark.read
      .option("sep", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/realestate.csv")
      .as[DecisionSchema]

    val assembler = new VectorAssembler().
      setInputCols(Array("HouseAge","DistanceToMRT", "NumberConvenienceStores")).
      setOutputCol("features")
    val df = assembler.transform(dsRaw)
        .select("PriceOfUnitArea","features")
     
    // Let's split our data into training data and testing data
    val trainTest = df.randomSplit(Array(0.5, 0.5))
    val trainingDF = trainTest(0)
    val testDF = trainTest(1)
    
    // Now create our linear regression model
    val lir = new DecisionTreeRegressor().setLabelCol("PriceOfUnitArea")

    // Train the model using our training data
    val model = lir.fit(trainingDF)
    
    // Now see if we can predict values in our test data.
    // Generate predictions using our linear regression model for all features in our 
    // test dataframe:
    val fullPredictions = model.transform(testDF).cache()
    
    // This basically adds a "prediction" column to our testDF dataframe.
    
    // Extract the predictions and the "known" correct labels.
    val predictionAndLabel = fullPredictions.select("prediction", "PriceOfUnitArea").collect()
    
    // Print out the predicted and actual values for each point
    for (prediction <- predictionAndLabel) {
      println(prediction)
    }
    
    // Stop the session
    spark.stop()

  }
}