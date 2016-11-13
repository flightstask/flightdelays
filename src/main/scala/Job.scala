import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

trait Job {
  def checkArguments(args: Array[String]) = if (args.length < 2) {
    throw new MissingArgumentsException(
      "Please provide input path and destination")
  }

  def getSession(name: String) = SparkSession
      .builder()
      .appName("Find busy airports")
      .getOrCreate()

  def saveCSV(input: DataFrame, dest: String) = {
    input.cache.show()

    input.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(dest.toString)
  }
}
