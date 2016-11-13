import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DelayProbability extends Job {
  val spark = getSession("Find busy airports")

  def main(args: Array[String]) {
    checkArguments(args)
    val minCount = if(args.length > 2) args(2).toInt else 0
    val flights = spark.read.parquet(args(0))
    val delayProbability = transform(flights, minCount)
    saveCSV(delayProbability, args(1))
  }

  def transform(input: DataFrame, minCount: Int = 0) = {
    import spark.implicits._

    val timeBucket = (UDFs.getHour($"DepTime") / 4).cast("int").as("DepartureTimeBlock")
    val delayBucket = when($"ArrDelay" <= 0, "OnTime")
      .when($"ArrDelay" <= 10, "UpTo10Minlate")
      .otherwise("MoreThan10MinLate")
      .as("DelayBucket")

    val knownInput = input
      .filter($"ArrDelay".isNotNull && $"DayOfWeek".isNotNull && timeBucket.isNotNull)

    val delayCounts = knownInput
      .groupBy($"Origin", $"Dest", $"DayOfWeek", delayBucket, timeBucket)
      .agg(count(lit(1)).as("CountPerGroup"))
      .cache

    val delayTotals = delayCounts
      .groupBy($"Origin", $"Dest", $"DayOfWeek", $"DepartureTimeBlock")
      .agg(sum($"CountPerGroup").as("TotalCount"))
      .filter($"TotalCount" > minCount)

    val delayProbability = delayCounts
      .join(delayTotals, Seq("Origin", "Dest", "DayOfWeek", "DepartureTimeBlock"))
      .withColumn("Probability", $"CountPerGroup" / $"TotalCount")

    delayProbability
      .groupBy($"Origin", $"Dest", $"DayOfWeek", $"DepartureTimeBlock", $"TotalCount")
      .pivot("DelayBucket")
      .agg(first($"Probability"))
      .na.fill(0, Seq("OnTime", "UpTo10Minlate", "MoreThan10MinLate"))
      .orderBy($"MoreThan10MinLate".desc)
      .limit(100)
  }

  def getHour(time: java.lang.Integer): Option[Int] = Option(time) match {
    case Some(time) if time < 2400 => Some(time / 100)
    case Some(time) if time == 2400 => Some(0)
    case _ => None
  }

  object UDFs {
    def getHour = udf(DelayProbability.getHour _)
  }
}
