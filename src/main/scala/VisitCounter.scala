import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

case class Provider(providerId: Long, name: String, specialty: String)
case class Visit(visitId: Long, providerId: Long)

object VisitCounter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Visit Counter")
      .getOrCreate()

    // Assuming providersDF and visitsDF are already loaded
    val providersDF = spark.read.option("multiline",true).json("providers.json")
    val visitsDF = spark.read.option("multiline",true).json("visits.json")

    import spark.implicits._

    // Calculate total visits per provider
    val visitsPerProviderDF = visitsDF
      .join(providersDF, visitsDF("providerId") === providersDF("providerId"))
      .groupBy(providersDF("providerId"), providersDF("name"), providersDF("specialty"))
      .agg(count(visitsDF("visitId")).alias("total_visits"))

    val visitsByMonthDF = visitsDF
      .join(providersDF, visitsDF("providerId") === providersDF("providerId"))
      .groupBy(providersDF("providerId"), visitsDF("visitMonth"))
      .agg(count(visitsDF("visitId")).alias("total_visits"))
    visitsDF.show()

    visitsByMonthDF.show()

    // Output the result partitioned by specialty in JSON format
    val outputPathBySpecialty = "output-by-specialty"
    val outputPathByMonth = "output-by-month"

    visitsPerProviderDF
      .write.mode(SaveMode.Overwrite)
      .partitionBy("specialty")
      .json(outputPathBySpecialty)

    visitsByMonthDF
      .write.mode(SaveMode.Overwrite)
      .json(outputPathByMonth)

    spark.stop()
  }
}

