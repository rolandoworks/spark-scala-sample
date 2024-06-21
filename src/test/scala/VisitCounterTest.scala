import org.scalatest.{BeforeAndAfterAll}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions._

class VisitCounterTest extends AnyFunSuite with BeforeAndAfterAll {

  // Define the Spark session outside tests
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    // Initialize Spark session before all tests
    spark = SparkSession.builder()
      .appName("VisitCounterTest")
      .master("local[1]") // Run Spark locally with as many threads as available cores
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    // Stop Spark session after all tests
    if (spark != null) {
      spark.stop()
    }
  }

  test("Calculate total visits per provider and partition by specialty") {
    // Test data
    val providersData = Seq(
      (1L, "Provider A", "Cardiology"),
      (2L, "Provider B", "Dermatology"),
      (3L, "Provider C", "Orthopedics")
    )
    val visitsData = Seq(
      (101L, 1L), (102L, 1L),
      (103L, 2L), (104L, 2L), (105L, 2L),
      (106L, 3L)
    )

    import spark.implicits._

    // Create DataFrames from test data
    val providersDF = providersData.toDF("providerId", "name", "specialty")
    val visitsDF = visitsData.toDF("visitId", "providerId")

    // Import the main logic from your application
    import VisitCounter.calculateTotalVisitsPerProvider

    // Execute the logic
    val resultDF = calculateTotalVisitsPerProvider(spark, providersDF, visitsDF)

    // Verify the result
    val expectedResults = Seq(
      (1L, "Provider A", "Cardiology", 2L),
      (2L, "Provider B", "Dermatology", 3L),
      (3L, "Provider C", "Orthopedics", 1L)
    ).toDF("providerId", "name", "specialty", "total_visits")

    assert(resultDF.except(expectedResults).count() == 0)
    assert(expectedResults.except(resultDF).count() == 0)
  }
}

