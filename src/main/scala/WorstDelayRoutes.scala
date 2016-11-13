import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, count, expr, when}

object WorstDelayRoutes extends Job {
  val spark = getSession("Find routes with highest delay count")

  def main(args: Array[String]) {
    checkArguments(args)
    val flights = spark.read.parquet(args(0))
    val worstDelayRoutes = transform(flights)
    saveCSV(worstDelayRoutes, args(1))
  }

  def transform(flights: DataFrame) = {
    import spark.implicits._
    val routeDelays = flights
      .groupBy($"Dest", $"Origin")
      .agg(
        count(when($"ArrDelay" > 0, $"ArrDelay")).as("DelayedFlightsCount"),
        count($"ArrDelay").as("TotalKnownFlights"))
      .withColumn("DelayProbability", $"DelayedFlightsCount" / $"TotalKnownFlights")

    val worstDelayRoutes = routeDelays
      .orderBy($"DelayedFlightsCount".desc)
      .limit(20)

    worstDelayRoutes
  }
}
