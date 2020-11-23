package monthlyMetrics

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{SparkSession, functions}

object monthlyRunner {
  def monthlyMain(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Patriots Monthly Metrics")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val years = Array(2020, 2019)
    var months = Array("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov")

    /*years.foreach(year => {
      var yearpath = "Patriots/" + year + "/"

      if (year == 2019)
      {
        months = months :+ "Dec"
      }

      months.foreach(month => {
        val filepath = yearpath + month

        //println(s"${month} ${year}")

        val df = spark.read.option("multiline", "true").json(filepath)
        val results = df.select(functions.explode(df("results")))

        results
          .withColumn("Month", lit(month + " " + year))
          .select("Month", "col.favorite_count", "col.reply_count", "col.retweet_count")
          .filter($"favorite_count" > 0)
          .filter($"reply_count" > 0)
          .filter($"retweet_count" > 0)
          .groupBy("Month")
          .agg(functions.avg("favorite_count"), functions.avg("reply_count"), functions.avg("retweet_count"))
          .show()

        results
          .withColumn("Month", lit(month + " " + year))
          .withColumn("Category", lit("Most replied"))
          .select("Month", "Category", "col.text", "col.reply_count")
          .sort(functions.desc("reply_count"))
          .show(1, false)

        results
          .withColumn("Month", lit(month + " " + year))
          .withColumn("Category", lit("Most retweeted"))
          .select("Month", "Category", "col.text", "col.retweet_count")
          .sort(functions.desc("retweet_count"))
          .show(1, false)

        results
          .withColumn("Month", lit(month + " " + year))
          .withColumn("Category", lit("Most favorited"))
          .select("Month", "Category", "col.text", "col.favorite_count")
          .sort(functions.desc("favorite_count"))
          .show(1, false)
      })
    })*/

    years.foreach(year => {
      var yearpath = "Patriots/PerYear/" + year

      val df = spark.read.option("multiline", "true").json(yearpath)
      val results = df.select(functions.explode(df("results")))

      results
        .withColumn("Year", lit(year))
        .select("Year", "col.favorite_count")
        .filter($"favorite_count" > 0)
        .groupBy("Year")
        .agg(functions.avg("favorite_count"))
        .show()

      results
        .withColumn("Year", lit(year))
        .select("Year", "col.reply_count")
        .filter($"reply_count" > 0)
        .groupBy("Year")
        .agg(functions.avg("reply_count"))
        .show()

      results
        .withColumn("Year", lit(year))
        .select("Year", "col.retweet_count")
        .filter($"retweet_count" > 0)
        .groupBy("Year")
        .agg(functions.avg("retweet_count"))
        .show()
    })
  }
}
