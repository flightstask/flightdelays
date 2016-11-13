import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalatest.{ BeforeAndAfterAll, FunSpec, Matchers }

class DelayProbabilityTest extends FunSpec with BeforeAndAfterAll with SharedSparkContext with Matchers {
  var result: DataFrame = _
  val precision = 0.00001

  override def beforeAll {
    super.beforeAll()
    import spark.implicits._
    val NA = null:java.lang.Integer
    val flights = Seq[(String, String, Int, java.lang.Integer, java.lang.Integer)](
      ("VNO", "SFO", 1, 15, 1908),
      ("VNO", "SFO", 1, 11, 1703),
      ("VNO", "SFO", 1,  5, 1855),
      ("VNO", "SFO", 1,  0, 1655),
      ("VNO", "SFO", 1,  2,   10),
      ("VNO", "SFO", 1,  1,  105),
      ("VNO", "SFO", 1, -2,  204),
      ("SFO", "VNO", 1, 25,  333),
      ("SFO", "VNO", 1,  5,  244),
      ("SFO", "VNO", 1, NA,  155),
      ("SFO", "VNO", 2, -1,  643),
      ("SFO", "VNO", 2,  2, 2601),
      ("SFO", "VNO", 2, -1,   NA)
    ).toDF("Origin", "Dest", "DayOfWeek", "ArrDelay", "DepTime")
    result = DelayProbability.transform(flights)
  }

  def getRoute(
      routes: DataFrame,
      origin: String,
      dest: String,
      dayOfWeek: Int,
      departureTimeBlock: Int): Row = {
    result
      .filter(col("Origin") === origin)
      .filter(col("Dest") === dest)
      .filter(col("DayOfWeek") === dayOfWeek)
      .filter(col("DepartureTimeBlock") === departureTimeBlock)
      .first
  }

  def checkProbabilities(route: Row, probabilties: (Double, Double, Double)) = {
    route.getAs[Double]("OnTime") shouldBe probabilties._1 +- precision
    route.getAs[Double]("UpTo10Minlate") shouldBe probabilties._2 +- precision
    route.getAs[Double]("MoreThan10MinLate") shouldBe probabilties._3 +- precision
  }

  describe("Delay Probability job") {

    it("finds correct delay probabilities for VNO - SFO late Monday flight") {
      checkProbabilities(
        route = getRoute(result, "VNO", "SFO", 1, 4),
        probabilties = (0.25, 0.25, 0.5))
    }

    it("finds correct delay probabilities for VNO - SFO early Monday flight") {
      checkProbabilities(
        route = getRoute(result, "VNO", "SFO", 1, 0),
        probabilties = (0.33333, 0.66666, 0.0))
    }

    it("finds correct delay probabilities for SFO - SNO Monday flight") {
      checkProbabilities(
        route = getRoute(result, "SFO", "VNO", 1, 0),
        probabilties = (0.0, 0.5, 0.5))
    }

    it("finds correct delay probabilities for SFO - VNO Tuesday flight") {
      checkProbabilities(
        route = getRoute(result, "SFO", "VNO", 2, 1),
        probabilties = (1.0, 0.0, 0.0))
    }

    it("should produce results ordered by MoreThan10MinLate probability") {
      result.collect().map(_.getAs[Double]("MoreThan10MinLate")).reverse shouldBe sorted
    }
  }

  describe("helpers") {
    describe("getHour") {
      it("should extract hour from HHmm format") {
        DelayProbability.getHour(2313) shouldBe Some(23)
        DelayProbability.getHour(15) shouldBe Some(0)
        DelayProbability.getHour(115) shouldBe Some(1)
      }

      it("should return 0 for 2400") {
        DelayProbability.getHour(2400) shouldBe Some(0)
      }

      it("should return None for invalid time") {
        DelayProbability.getHour(2613) shouldBe None
        DelayProbability.getHour(9999) shouldBe None
      }

      it("should handle null values") {
        DelayProbability.getHour(null:java.lang.Integer) shouldBe None
      }
    }
  }
}
