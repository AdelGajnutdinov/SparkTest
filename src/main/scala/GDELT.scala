import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

class GDELT(sparkSession: SparkSession, input: String) {
    var gdeltSchema = StructType(Array(
      StructField("GLOBALEVENTID", IntegerType, true),
      StructField("SQLDATE", IntegerType, true),
      StructField("MonthYear", IntegerType, true),
      StructField("Year", IntegerType, true),
      StructField("FractionDate", DoubleType, true),
      StructField("Actor1Code", StringType, true),
      StructField("Actor1Name", StringType, true),
      StructField("Actor1CountryCode", StringType, true),
      StructField("Actor1KnownGroupCode", StringType, true),
      StructField("Actor1EthnicCode", StringType, true),
      StructField("Actor1Religion1Code", StringType, true),
      StructField("Actor1Religion2Code", StringType, true),
      StructField("Actor1Type1Code", StringType, true),
      StructField("Actor1Type2Code", StringType, true),
      StructField("Actor1Type3Code", StringType, true),
      StructField("Actor2Code", StringType, true),
      StructField("Actor2Name", StringType, true),
      StructField("Actor2CountryCode", StringType, true),
      StructField("Actor2KnownGroupCode", StringType, true),
      StructField("Actor2EthnicCode", StringType, true),
      StructField("Actor2Religion1Code", StringType, true),
      StructField("Actor2Religion2Code", StringType, true),
      StructField("Actor2Type1Code", StringType, true),
      StructField("Actor2Type2Code", StringType, true),
      StructField("Actor2Type3Code", StringType, true),
      StructField("IsRootEvent", IntegerType, true),
      StructField("EventCode", StringType, true),
      StructField("EventBaseCode", StringType, true),
      StructField("EventRootCode", StringType, true),
      StructField("QuadClass", IntegerType, true),
      StructField("GoldsteinScale", DoubleType, true),
      StructField("NumMentions", IntegerType, true),
      StructField("NumSources", IntegerType, true),
      StructField("NumArticles", IntegerType, true),
      StructField("AvgTone", DoubleType, true),
      StructField("Actor1Geo_Type", IntegerType, true),
      StructField("Actor1Geo_FullName", StringType, true),
      StructField("Actor1Geo_CountryCode", IntegerType, true),
      StructField("Actor1Geo_ADM1Code", StringType, true),
      StructField("Actor1Geo_Lat", FloatType, true),
      StructField("Actor1Geo_Long", FloatType, true),
      StructField("Actor1Geo_FeatureID", IntegerType, true),
      StructField("Actor2Geo_Type", IntegerType, true),
      StructField("Actor2Geo_FullName", StringType, true),
      StructField("Actor2Geo_CountryCode", StringType, true),
      StructField("Actor2Geo_ADM1Code", StringType, true),
      StructField("Actor2Geo_Lat", FloatType, true),
      StructField("Actor2Geo_Long", FloatType, true),
      StructField("Actor2Geo_FeatureID", IntegerType, true),
      StructField("ActionGeo_Type", IntegerType, true),
      StructField("ActionGeo_FullName", StringType, true),
      StructField("ActionGeo_CountryCode", StringType, true),
      StructField("ActionGeo_ADM1Code", StringType, true),
      StructField("ActionGeo_Lat", FloatType, true),
      StructField("ActionGeo_Long", FloatType, true),
      StructField("ActionGeo_FeatureID", IntegerType, true),
      StructField("DATEADDED", IntegerType, true),
      StructField("SOURCEURL", StringType, true)))

    //Read CSV file to DF and define scheme on the fly
    private val gdelt = sparkSession.read
      .option("header", "true")
      .option("delimiter", "\t")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .schema(gdeltSchema)
      .csv(input)

    gdelt.createOrReplaceTempView("gdeltTable")

  //Find the 10 most mentioned actors (persons)
    def mostMentionedActors(): DataFrame = {
      sparkSession.sql(
        " SELECT Actor1Code, first(Actor1Name), COUNT(*) count" +
          " FROM gdeltTable" +
          " WHERE SOURCEURL IS NOT NULL" +
          " GROUP BY Actor1Code ORDER BY count DESC LIMIT 25")
  }
}
