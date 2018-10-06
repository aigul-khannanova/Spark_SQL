import org.apache.spark.sql.types.{FloatType, _}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

    object DataFrames {

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

      def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val sparkSession = SparkSession
          .builder()
          .appName("Spark_Data_Frames")
          .master("local[*]")
          .getOrCreate();

        val F_gdel = "C:\\Users\\Бонифация\\AppData\\Local\\Temp\\Spark_SQL\\src\\main\\resources\\F_gdelt.csv"
        val F_cameo = "C:\\Users\\Бонифация\\AppData\\Local\\Temp\\Spark_SQL\\src\\main\\resources\\F_cameo.csv"


        //Read CSV file to DF and define scheme on the fly
        val GDELL = sparkSession.read
          .option("header", false)
          .option("delimiter", "\t")
          .option("nullValue", "")
          .option("treatEmptyValuesAsNulls", true)
          .schema(gdeltSchema).csv(F_gdel)

        val CAMEOO = sparkSession.read
          .option("header", true)
          .option("delimiter", "\t")
          .option("nullValue", "")
          .option("treatEmptyValuesAsNulls", true)
          .csv(F_cameo)


        // Register the DataFrame as a SQL temporary view
        val gde = GDELL.createOrReplaceTempView("GDELT")
        val cam = CAMEOO.createOrReplaceTempView("CAM")


        GDELL.show(100)
        GDELL.printSchema()
        CAMEOO.show(100)
        CAMEOO.printSchema()


    println("List all events which took place in recent time in USA, RUSSIA")
    sparkSession.sql("SELECT GLOBALEVENTID, EventCode, ActionGeo_CountryCode,ActionGeo_Lat,ActionGeo_Long" +
      " FROM GDELT WHERE ActionGeo_CountryCode='US' OR ActionGeo_CountryCode='RS'").show(50,false)

    println("Find the 10 most mentioned actors (persons)")
    sparkSession.sql("SELECT Actor1Name,COUNT(Actor1Name) FROM GDELT GROUP BY Actor1Name ORDER BY COUNT(Actor1Name) DESC").show(10)
    sparkSession.sql("SELECT Actor2Name,COUNT(Actor1Name) FROM GDELT GROUP BY Actor2Name ORDER BY COUNT(Actor2Name) DESC").show(10)

    println("Join EventCodes with CameoCodes in order to get events' description")
    sparkSession.sql("SELECT DISTINCT GDELT.GLOBALEVENTID, CAM.EventDescription " +
      "FROM GDELT JOIN CAM ON " +
      "GDELT.EventCode = CAM.CAMEOcode ").show(10,false)

        println("Find the 10 most mentioned events with description")
    sparkSession.sql("SELECT GLOBALEVENTID,NumMentions,EventDescription " +
      "FROM GDELT,CAMEO " +
      "WHERE EventCode=CAMEOcode ORDER BY NumMentions DESC ").show(10,false)

      }
}
