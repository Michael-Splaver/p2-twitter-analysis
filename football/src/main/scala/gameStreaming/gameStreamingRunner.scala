package gameStreaming

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.nio.file.{Files, Paths}
import java.util
import java.util.{ArrayList, HashMap}

import org.apache.http.NameValuePair
import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair
import org.apache.spark.sql.{SparkSession, functions}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object gameStreamingRunner {
  def streamMain(args: Array[String]): Unit = {

    val bearerToken = System.getenv("BEARER_TOKEN")

    if (null != bearerToken)
    {
      val rules = new HashMap[String, String]()
      rules.put("from:AZCardinals", "FromCardinals")
      rules.put("to:AZCardinals", "ToCardinals")
      rules.put("from:Seahawks", "FromSeahawks")
      rules.put("to:Seahawks", "ToSeahawks")
      rules.put("from:NFL", "FromNFL")
      rules.put("to:NFL", "ToNFL")
      /*rules.put("from:realDonaldTrump", "FromTrump")
      rules.put("to:RealDonaldTrump", "ToTrump")
      rules.put("from:JoeBiden", "FromBiden")
      rules.put("to:JoeBiden", "ToBiden")*/
      setupRules(bearerToken, rules)
    }

    Future {
      tweetStreamToDir(bearerToken, linesPerFile = 50)
    }

    val spark = SparkSession.builder()
      .appName("Game Twitter Stream")
      .master("local[4]")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val staticDf = spark.read.json("sample")

    val streamDf = spark.readStream.schema(staticDf.schema).json("twitterstream")

    streamDf
      .select(functions.explode(streamDf("matching_rules"))).select("col.tag")
      .as[String]
      .groupBy("tag")
      .count()
      .sort(functions.desc("count"))
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()
  }

  def tweetStreamToDir(bearerToken: String, dirname: String = "twitterstream", linesPerFile: Int = 1000): Unit = {

    var parameters = new ArrayList[NameValuePair]()
    parameters.add(new BasicNameValuePair("expansions", "author_id"))
    parameters.add(new BasicNameValuePair("tweet.fields", "created_at"))

    val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build
    val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream")
    uriBuilder.addParameters(parameters)
    val httpGet = new HttpGet(uriBuilder.build)
    httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken))
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity

    if (null != entity)
    {
      val reader = new BufferedReader(new InputStreamReader(entity.getContent))
      var line = reader.readLine
      var fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
      var lineNumber = 1
      var fileNum = 1

      while (line != null)
      {
        if (lineNumber % linesPerFile == 0)
        {
          fileWriter.close()
          Files.move(
            Paths.get("tweetstream.tmp"),
            Paths.get(s"${dirname}/tweetstream-part${fileNum}"))
          fileNum += 1
          fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
        }

        val output = line
        //println(output)

        if (line != "")
        {
          fileWriter.println(line)
          lineNumber += 1
        }

        line = reader.readLine()
      }
    }
  }

  import java.io.IOException
  import java.net.URISyntaxException

  @throws[IOException]
  @throws[URISyntaxException]
  private def setupRules(bearerToken: String, rules: HashMap[String, String]): Unit = {
    val existingRules = getRules(bearerToken)

    if (existingRules.size > 0)
    {
      deleteRules(bearerToken, existingRules)
    }

    createRules(bearerToken, rules)
  }

  import org.apache.http.client.config.CookieSpecs
  import org.apache.http.client.config.RequestConfig
  import org.apache.http.client.methods.HttpPost
  import org.apache.http.client.utils.URIBuilder
  import org.apache.http.entity.StringEntity
  import org.apache.http.impl.client.HttpClients
  import org.apache.http.util.EntityUtils

  @throws[URISyntaxException]
  @throws[IOException]
  private def createRules(bearerToken: String, rules: HashMap[String, String]): Unit = {
    val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build
    val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules")
    val httpPost = new HttpPost(uriBuilder.build)
    httpPost.setHeader("Authorization", String.format("Bearer %s", bearerToken))
    httpPost.setHeader("content-type", "application/json")
    val body = new StringEntity(getFormattedString("{\"add\": [%s]}", rules))
    httpPost.setEntity(body)
    val response = httpClient.execute(httpPost)
    val entity = response.getEntity

    if (null != entity)
    {
      System.out.println(EntityUtils.toString(entity, "UTF-8"))
    }
  }

  private def getFormattedString(string: String, ids: util.ArrayList[String]) = {
    val sb = new StringBuilder
    if (ids.size == 1) String.format(string, "\"" + ids.get(0) + "\"")
    else {
      import scala.collection.JavaConversions._
      for (id <- ids) {
        sb.append("\"" + id + "\"" + ",")
      }
      val result = sb.toString
      String.format(string, result.substring(0, result.length - 1))
    }
  }

  private def getFormattedString(string: String, rules: util.Map[String, String]) = {
    val sb = new StringBuilder
    if (rules.size == 1) {
      val key = rules.keySet.iterator.next
      String.format(string, "{\"value\": \"" + key + "\", \"tag\": \"" + rules.get(key) + "\"}")
    }
    else {
      import scala.collection.JavaConversions._
      for (entry <- rules.entrySet) {
        val value = entry.getKey
        val tag = entry.getValue
        sb.append("{\"value\": \"" + value + "\", \"tag\": \"" + tag + "\"}" + ",")
      }
      val result = sb.toString
      String.format(string, result.substring(0, result.length - 1))
    }
  }

  private def getRules(bearerToken: String): util.ArrayList[String] = {
    var rules = new util.ArrayList[String]()
    val httpClient = HttpClients.custom()
      .setDefaultRequestConfig(RequestConfig.custom()
        .setCookieSpec(CookieSpecs.STANDARD).build())
      .build();

    val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules")

    val httpGet = new HttpGet(uriBuilder.build())
    httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken))
    httpGet.setHeader("content-type", "application/json")
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity()

    if (null != entity)
    {
      val ids = (EntityUtils.toString(entity, "UTF-8").split("id\":\""))

      for (i <- 1 to ids.length - 1)
      {
        rules.add(ids(i).substring(0, ids(i).indexOf("\"")))
      }
    }

    rules
  }

  import org.apache.http.client.config.CookieSpecs
  import org.apache.http.client.config.RequestConfig
  import org.apache.http.client.methods.HttpPost
  import org.apache.http.client.utils.URIBuilder
  import org.apache.http.entity.StringEntity
  import org.apache.http.impl.client.HttpClients
  import org.apache.http.util.EntityUtils
  import java.util

  @throws[URISyntaxException]
  @throws[IOException]
  private def deleteRules(bearerToken: String, existingRules: util.ArrayList[String]): Unit = {
    val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build
    val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules")
    val httpPost = new HttpPost(uriBuilder.build)
    httpPost.setHeader("Authorization", String.format("Bearer %s", bearerToken))
    httpPost.setHeader("content-type", "application/json")
    val body = new StringEntity(getFormattedString("{ \"delete\": { \"ids\": [%s]}}", existingRules))
    httpPost.setEntity(body)
    val response = httpClient.execute(httpPost)
    val entity = response.getEntity
    if (null != entity) System.out.println(EntityUtils.toString(entity, "UTF-8"))
  }
}