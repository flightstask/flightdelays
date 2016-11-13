import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.scalatest.{ BeforeAndAfterAll, FunSpec, Matchers }

class BusyAirportsTest extends FunSpec with SharedSparkContext with BeforeAndAfterAll with Matchers {
  var result: DataFrame = _

  override def beforeAll {
    super.beforeAll()
    import spark.implicits._
    val flights = (
      Seq.fill(90)(("VNO", "SFO")) ++
        Seq.fill(20)(("SAN", "SFO")) ++
        Seq.fill(5)(("SFO", "VNO")) ++
        Seq.fill(2)(("FDO", "VNO"))
    ).toDF("Origin", "Dest")
    result = BusyAirports.transform(flights)
  }

  describe("Busy Airports job") {
    it("should produce total flight counts per airport") {
      result.collect() should contain allOf(
        Row("SFO", 115),
        Row("SAN", 20),
        Row("VNO", 97),
        Row("FDO", 2))
    }

    it("should produce ordered results") {
      result.collect().map(_.getAs[Long]("TotalFlights")).reverse shouldBe sorted
    }
  }
}
