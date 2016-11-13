import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import java.util.Date
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SharedSparkContext extends BeforeAndAfterAll { this: Suite =>
  var _spark: SparkSession = _
  lazy val spark: SparkSession = _spark

  override protected def beforeAll(): Unit = {
    _spark = SparkSession.builder
      .master("local")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try {
      spark.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }
}
