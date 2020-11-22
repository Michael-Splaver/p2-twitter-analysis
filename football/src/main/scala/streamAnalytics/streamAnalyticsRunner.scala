package streamAnalytics

import org.apache.spark.sql.{SparkSession, functions}

object streamAnalyticsRunner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Hello Spark SQL")
      .master("local[4]")
      .config("spark.sql.streaming.checkpointLocation", "checkpoint")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val staticDf = spark.read.json("sample")

    val streamDf = spark.readStream.schema(staticDf.schema).json("twitterstream")

    streamDf
      .select($"data.created_at", $"matching_rules.tag")
      .as[Tweet]
      .map(
        tweet => (tweet.created_at.substring(tweet.created_at.indexOf("T") + 1,
          tweet.created_at.indexOf("T") + 6), tweet.tag(0)))
      .select($"_1".as("Time"), $"_2".as("Tag"))
      .filter($"Tag" === "ToCardinals")
      .groupBy("Time")
      .count()
      .sort(functions.asc("Time"))
      .writeStream
      .option("numRows", 1000)
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()

    /*streamDf
      .select($"data.created_at", $"matching_rules.tag")
      .as[Tweet]
      .map(
        tweet => (tweet.created_at.substring(tweet.created_at.indexOf("T") + 1,
          tweet.created_at.indexOf("T") + 6), tweet.tag(0)))
      .select($"_1".as("Time"), $"_2".as("Tag"))
      .filter($"Tag" === "ToSeahawks")
      .groupBy("Time")
      .count()
      .sort(functions.asc("Time"))
      .writeStream
      .option("numRows", 1000)
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()*/

    /*streamDf
      .select($"data.created_at", $"matching_rules.tag")
      .as[Tweet]
      .map(
        tweet => (tweet.created_at.substring(tweet.created_at.indexOf("T") + 1,
          tweet.created_at.indexOf("T") + 6), tweet.tag(0)))
      .select($"_1".as("Time"), $"_2".as("Tag"))
      .filter($"Tag" === "ToNFL")
      .groupBy("Time")
      .count()
      .sort(functions.asc("Time"))
      .writeStream
      .option("numRows", 1000)
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()*/

    /*streamDf
      .select($"data.created_at", $"matching_rules.tag")
      .as[Tweet]
      .map(
        tweet => (tweet.created_at.substring(tweet.created_at.indexOf("T") + 1,
          tweet.created_at.indexOf("T") + 6), tweet.tag(0)))
      .select($"_1".as("Time"), $"_2".as("Tag"))
      .filter($"Tag" === "FromCardinals")
      .groupBy("Time")
      .count()
      .sort(functions.asc("Time"))
      .writeStream
      .option("numRows", 1000)
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()*/

    /*streamDf
      .select($"data.created_at", $"matching_rules.tag")
      .as[Tweet]
      .map(
        tweet => (tweet.created_at.substring(tweet.created_at.indexOf("T") + 1,
          tweet.created_at.indexOf("T") + 6), tweet.tag(0)))
      .select($"_1".as("Time"), $"_2".as("Tag"))
      .filter($"Tag" === "FromSeahawks")
      .groupBy("Time")
      .count()
      .sort(functions.asc("Time"))
      .writeStream
      .option("numRows", 1000)
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()*/

    /*streamDf
      .select($"data.created_at", $"matching_rules.tag")
      .as[Tweet]
      .map(
        tweet => (tweet.created_at.substring(tweet.created_at.indexOf("T") + 1,
          tweet.created_at.indexOf("T") + 6), tweet.tag(0)))
      .select($"_1".as("Time"), $"_2".as("Tag"))
      .filter($"Tag" === "FromNFL")
      .groupBy("Time")
      .count()
      .sort(functions.asc("Time"))
      .writeStream
      .option("numRows", 1000)
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()*/

    //val results = df.select(functions.explode())

    //results.show(100, false)

    //println(results.count())

    //val ds = results.as[Tweet]

    /*val resultsds = ds
      .map(
        tweet => (tweet.created_at.substring(tweet.created_at.indexOf("T") + 1,
          tweet.created_at.indexOf("T") + 6), tweet.tag))
      .sort(functions.desc("_1"))
      .select($"_1".as("Time"), $"_2".as("Tag"))

    resultsds.show(200, false)*/

    /*resultsds.createOrReplaceTempView("TagTimes")

    spark.sql("SELECT Time, COUNT(Tag) AS ToCardinals" +
      " FROM TagTimes" +
      " WHERE Tag='ToCardinals'" +
      " GROUP BY Time" +
      " ORDER BY Time DESC").show(10, false)*/
  }

  case class Tweet(created_at: String, tag: Array[String]) {}
}
