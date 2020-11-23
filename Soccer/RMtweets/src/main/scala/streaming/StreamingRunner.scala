package streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Duration, StreamingContext}


object StreamingRunner {

  def main(args: Array[String]): Unit ={

    //SparkStreaming uses DStreams, which are similar to RDDs

    // To interact with SparkStreaming, we create a StreamingContext
    // that produces DStreams in the same way SparkContext produces RDDs.

    //It is worth explicitly mentioning that these APIs are unified --
    //DStreams and StructuredStreaming both use RDDs under the hood
    //but StructuredStreaming does not make use of DStreams

    val eMap = System.getenv()
    System.setProperty("twitter4j.oauth.consumerKey", eMap.get("CONSUMER_KEY"))
    System.setProperty("twitter4j.oauth.consumerSecret", eMap.get("CONSUMER_SECRET"))
    System.setProperty("twitter4j.oauth.accessToken", eMap.get("ACCESS_TOKEN"))
    System.setProperty("twitter4j.oauth.accessTokenSecret", eMap.get("ACCESS_TOKEN_SECRET"))


    val sparkConf = new SparkConf().setAppName("Twitter SparkStreaming").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")

    // StreamingContext in lieu of SparkContext
    // We are specifying 1 second duration batches, meaning each second we will produce an RDD
    // and run our analysis

    val filter: Seq[String]= Seq("@realmadrid")
    val ssc = new StreamingContext(sc, Duration(1000)) //EVERY 1 SECOND
    //This is a DStream, it is a stream of RDDs
    val stream = TwitterUtils.createStream(ssc, None, filter)

    val hashTags = stream.flatMap(status => {status.getText.split("\\s").filter(_.startsWith("#"))})

    // get the top hashtag count per 60 secs:
    val topCounts60 = hashTags.map((_,1)).reduceByKeyAndWindow(_+_, Duration (60000))
      .map({case (topic, count) => (count, topic)} ) //swap order so we can sort by key
      .transform(_.sortByKey(false))

    //On our stream we can take actions, like forEachRDD

    topCounts60.foreachRDD(rdd => {
      rdd.take(10).foreach(println)
      println("-----------------------------------")})



    //To get this to run we call .start() on out StreamingContext, then awaitTermination
    //to keep the main threads alive.

    ssc.start()
    // waits for termination -- will not actually end so we will have to kill it.
    ssc.awaitTermination()
  }

}
