import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.sum

object BusyAirports extends Job {
  val spark = getSession("Find busy airports")

  def main(args: Array[String]) {
    checkArguments(args)
    val flights = spark.read.parquet(args(0))
    val busiestAirports = transform(flights)
    saveCSV(busiestAirports, args(1))
  }

  def transform(input: DataFrame) = {
    import spark.implicits._
    val incoming = input.groupBy($"Dest".as("Airport")).count
    val outgoing = input.groupBy($"Origin".as("Airport")).count

    val countsPerAirport = incoming.unionAll(outgoing)
      .groupBy($"Airport")
      .agg(sum($"count").as("TotalFlights"))

    val busiestAirports = countsPerAirport
      .orderBy($"TotalFlights".desc)
      .limit(20)

    busiestAirports
  }
}
