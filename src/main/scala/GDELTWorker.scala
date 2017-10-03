import org.apache.spark.sql.SparkSession

object GDELTWorker {
  def main(args: Array[String]): Unit = {
    val gdeltFile = "C:/dataForScalaProjects/gdelt.csv"
    val cameoFile = "C:/dataForScalaProjects/CAMEO_event_codes.csv"
    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-basic")
      .master("local[*]")
      .getOrCreate()
    //Initialize GDELT DataFrame
    val gdelt = new GDELT(sparkSession, gdeltFile)
    //gdelt.mostMentionedActors().show(50)

    val mostMentionedEvents = gdelt.mostMentionedEvents()
    val cameoCodes = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .csv(cameoFile)
    mostMentionedEvents.join(cameoCodes,
      mostMentionedEvents.col("EventCode") === cameoCodes.col("CAMEOcode"), "inner")
      .select("EventCode", "EventDescription", "count")
      .show(25)
  }
}
