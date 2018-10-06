import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark_SQL {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkSession = SparkSession
      .builder()
      .appName("sparkSQL")
      .master("local[*]")
      .getOrCreate();

    val Tweets = sparkSession.read.json("C:\\Users\\Бонифация\\IdeaProjects\\SparkTest\\src\\main\\resources\\SQL.json")
    Tweets.show(20)
    Tweets.printSchema();

    // Register the DataFrame as a SQL temporary view
    Tweets.createOrReplaceTempView("tweetTable")

  //Find all the tweets by user
    println("All the tweets by user")
    sparkSession.sql("SELECT actor.displayName AS Name, body AS Tweet FROM tweetTable " +
      "GROUP BY actor.displayName,body ORDER BY actor.displayName ").show(150, false)


    //Get earliest and latest tweet dates
    println("Earliest and latest tweet dates")
    sparkSession.sql("SELECT object.postedTime AS Earliest_dates FROM tweetTable " +
      "WHERE object.postedTime IS NOT NULL ORDER BY object.postedTime ASC").show(5)
    sparkSession.sql("SELECT object.postedTime AS Latest_dates FROM tweetTable " +
      "WHERE object.postedTime IS NOT NULL ORDER BY object.postedTime DESC").show(5)

   // Count the most active languages
    println("The most active languages")
    sparkSession.sql("Select actor.languages, COUNT(actor.languages)" +
      "from tweetTable where actor.languages is not null " +
      "group by actor.languages order by COUNT(actor.languages) DESC").show(50,false)

  }
}

