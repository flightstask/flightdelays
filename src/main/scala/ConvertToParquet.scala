import org.apache.spark.sql.SparkSession

object ConvertToParquet extends Job {
  val spark =  getSession("Convert CSV data to Parquet")

  def main(args: Array[String]) {
    checkArguments(args)

    val flights = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NA")
      .load(args(0))

    flights.write.mode("overwrite").parquet(args(1))
  }
}
