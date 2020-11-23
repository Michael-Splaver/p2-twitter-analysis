package testing

import com.danielasfregola.twitter4s.{TwitterRestClient, TwitterStreamingClient}
import com.danielasfregola.twitter4s.entities.streaming.StreamingMessage
import com.danielasfregola.twitter4s.entities.{AccessToken, ConsumerToken, Tweet}
import net.liftweb
import net.liftweb.json.{DefaultFormats, JObject}
import org.apache.spark.sql.{SparkSession, functions}
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, Future}

object Runner {

  def main(args: Array[String]): Unit = {

    val consumerToken = ConsumerToken(key = System.getenv().get("CONSUMER_KEY"), System.getenv().get("CONSUMER_SECRET"))
    val accessToken = AccessToken(key = System.getenv().get("ACCESS_TOKEN"), secret = System.getenv().get("ACCESS_TOKEN_SECRET"))

    val appName = "p2-group5-twitter-analysis"

    val spark = SparkSession.builder()
      .appName("Hello Spark SQL")
      .master("local[8]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val client = TwitterRestClient(consumerToken, accessToken)
    val userName = "Phillies"
    userTimelineToFile(client, userName)

    val staticDf = spark.read.option("multiline", "true").json(userName)

    staticDf
      .select($"retweet_count", $"favorite_count",
        functions.to_date(functions.from_unixtime($"created_at"/1000,"yyyy-MM-dd HH:mm:ss.SSSS")).as("date"))
      .filter(functions.isnull($"retweeted_status"))
      .groupBy($"date")
      .agg(functions.count($"date").as("tweet_count"),
        functions.sum($"favorite_count").as("fav_sum"),
        functions.round(functions.avg($"favorite_count"),2).as("fav_avg"),
        functions.sum($"retweet_count").as("retweet_sum"),
        functions.round(functions.avg("retweet_count"),2).as("retweet_avg"))
      .sort(functions.asc("date"))
      .coalesce(1).write.option("header","true")
      .csv(s"$userName/Out/${userName.toLowerCase()}-stats-per-day")

    staticDf
      .select($"retweet_count", $"favorite_count",
        functions.date_format(functions.to_date(functions.from_unixtime($"created_at"/1000,"yyyy-MM")),"yyyy-MM").as("date"))
      .filter(functions.isnull($"retweeted_status"))
      .groupBy($"date")
      .agg(functions.count($"date").as("tweet_count"),
        functions.sum($"favorite_count").as("fav_sum"),
        functions.round(functions.avg($"favorite_count"),2).as("fav_avg"),
        functions.sum($"retweet_count").as("retweet_sum"),
        functions.round(functions.avg("retweet_count"),2).as("retweet_avg"))
      .sort(functions.asc("date"))
      .coalesce(1).write.option("header","true").csv(s"$userName/Out/${userName.toLowerCase()}-stats-per-month")

      //STREAMING DISABLED TO ALLOW PROGRAM TO FINISH
//    val streamingClient = TwitterStreamingClient(consumerToken, accessToken)
//
//    filteredStreaming(streamingClient)
//
//    val streamingDf = spark.read.option("multiline", "true").json("Streaming")
//
//    streamingDf
//      .select($"text")
//      .as[String]
//      .flatMap(_.toLowerCase.split("\\s"))
//      .map {
//        case "bluejays" =>
//          "Blue Jays"
//        case "blue jays" =>
//          "Blue Jays"
//        case "dodgers" =>
//          "Dodgers"
//        case "phillies" =>
//          "Phillies"
//        case "cubs" =>
//          "Cubs"
//        case "redSox" =>
//          "RedSox"
//        case "red sox" =>
//          "RedSox"
//        case "yankees" =>
//          "Yankees"
//        case noMatch =>
//          "no team mention"
//      }
//      .groupBy("value")
//      .count()
//      .sort(functions.desc("count"))
//      .show()
  }

  def filteredStreaming(client: TwitterStreamingClient): Unit = {

    var list = ListBuffer.newBuilder[Tweet]
    var tweetNumber = 0

    def tweetStreamToFile: PartialFunction[StreamingMessage, Unit] = {
      case tweet: Tweet =>
        if (tweetNumber < 100){
          list += tweet
          tweetNumber += 1
        }
        else{
          val jarr: Seq[Tweet] = list.result
          val timestamp = System.currentTimeMillis()
          fileSupport.toFileAsJson(s"Streaming/$timestamp-stream.json",jarr)

          list = ListBuffer.newBuilder[Tweet]
          list += tweet
          tweetNumber = 1
        }
        println(s"new tweet: ${tweet.text}\n")
    }

    Future{client.filterStatuses(stall_warnings = true, tracks = Seq("BlueJays", "Blue Jays", "Dodgers", "Phillies", "Cubs", "RedSox", "Red Sox", "Yankees"))(tweetStreamToFile)}
  }

  def userTimelineToFile(client: TwitterRestClient, username: String): Unit = {

    implicit val formats: DefaultFormats.type = DefaultFormats

    var counter = 0
    var max: Option[Long]= None

    while (counter <= 16){
      val future = client.userTimelineForUser(screen_name = username, max_id = max).map { ratedData =>
        val data = ratedData.data
        fileSupport.toFileAsJson(s"$username/${username.toLowerCase()}-timeline-$counter.json",data)
      }
      Await.result(future, Duration(5, SECONDS))

      val jsonString = fileSupport.readFromFile(s"$username/${username.toLowerCase()}-timeline-$counter.json")
      val json = liftweb.json.parse(jsonString)

      val tweetList = json.extract[List[JObject]]
      max = (tweetList.last \ "id").extractOpt[Long]
      if(max.isDefined) max = Some(max.get - 1)

      counter += 1
    }
    closeStream(client)
  }

  def closeStream(client: TwitterRestClient): Unit ={
    client.shutdown.onComplete(_ => println("closed client"))
  }
}
