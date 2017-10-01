import org.apache.spark.sql.SparkSession

object TweetsSqlQueries {
  def main(args: Array[String]): Unit = {
    val jsonFile = "C:/dataForScalaProjects/sampleTweets.json"

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-tweets")
      .master("local[*]")
      .getOrCreate()

    //Read json file to DF
    val tweetsDF = sparkSession.read.json(jsonFile)

    //Show the first 100 rows
    //tweetsDF.show(100);

    //Show thw scheme of DF
    tweetsDF.printSchema()

    // Register the DataFrame as a SQL temporary view
    tweetsDF.createOrReplaceTempView("tweetTable")

    //Get the actor name and body of messages in Russian
//    val sqlResult = sparkSession.sql(
//      " SELECT actor.displayName, body" +
//        " FROM tweetTable WHERE body IS NOT NULL" +
//        " AND twitter_lang = 'ru'")

    //Show only body of tweets
    //sqlResult.rdd.map(r => r(1)).foreach(println)

    //Get the most popular languages
//    sparkSession.sql(
//      " SELECT actor.languages, COUNT(*) as cnt" +
//        " FROM tweetTable " +
//        " GROUP BY actor.languages ORDER BY cnt DESC LIMIT 25")
//      .show(100)

    //Top devices used among all Twitter users
//    sparkSession.sql(
//      " SELECT generator.displayName, COUNT(*) count" +
//        " FROM tweetTable " +
//        " WHERE  generator.displayName IS NOT NULL" +
//        " GROUP BY generator.displayName ORDER BY count DESC LIMIT 25")
//      .show(100)

    //Top users who has a lot of friends
//    sparkSession.sql(
//      " SELECT actor.displayName, actor.friendsCount" +
//        " FROM tweetTable " +
//        " ORDER BY actor.friendsCount DESC LIMIT 25")
//      .show(100)

    //Find how many tweets each user has
    sparkSession.sql(
      " SELECT actor.id, first(actor.displayName), COUNT(*) count" +
        " FROM tweetTable" +
        " WHERE body IS NOT NULL" +
        " GROUP BY actor.id ORDER BY count DESC LIMIT 25")
      .show(100)
  }
}
