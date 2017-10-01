import org.apache.spark.sql.SparkSession

object GDELTWorker {
  def main(args: Array[String]): Unit = {

    var gdeltFile = "C:/dataForScalaProjects/gdelt.csv";

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-basic")
      .master("local[*]")
      .getOrCreate()

    //Initialize GDELT DataFrame
    val gdelt = new GDELT(sparkSession, gdeltFile)

    gdelt.mostMentionedActors().show(50)
  }
}
