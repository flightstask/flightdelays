import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.scalatest.{ BeforeAndAfterAll, FunSpec, Matchers }

class WorstDelayRoutesTest extends FunSpec with SharedSparkContext with BeforeAndAfterAll with Matchers {
  var result: DataFrame = _

  override def beforeAll {
    super.beforeAll()
    import spark.implicits._
    val NA = null:java.lang.Integer
    val flights = Seq[(String, String, java.lang.Integer)](
      ("VNO", "SFO", -10),
      ("VNO", "SFO", 10),
      ("VNO", "SFO", 0),
      ("VNO", "SFO", 2),
      ("VNO", "SFO", 1),
      ("VNO", "SFO", NA),
      ("SFO", "VNO", 5),
      ("SFO", "VNO", 2),
      ("SFO", "VNO", -1),
      ("VNO", "PAR", 1),
      ("VNO", "PAR", -10),
      ("BVA", "ORY", NA)
    ).toDF("Origin", "Dest", "ArrDelay")
    result = WorstDelayRoutes.transform(flights)
      .select("Origin", "Dest", "DelayedFlightsCount", "TotalKnownFlights")
  }

  describe("WorstDelayRoutes Airports job") {
    it("counts delayed flights") {
      result.collect() should contain allOf(
        Row("VNO", "SFO", 3, 5),
        Row("SFO", "VNO", 2, 3),
        Row("VNO", "PAR", 1, 2),
        Row("BVA", "ORY", 0, 0))
    }

    it("should produce ordered results") {
      result.collect().map(_.getAs[Long]("DelayedFlightsCount")).reverse shouldBe sorted
    }
  }
}
