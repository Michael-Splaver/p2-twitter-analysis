package historical

import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object Runner {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RMtweets")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
    //println("hello world")

    //------------------------------------2019-----------------------------------------

    val feb1_19df = spark.read.option("multiline", "true").json("Feb_19")
    val feb2_19df = spark.read.option("multiline", "true").json("Feb2_19")

    val march1_19df = spark.read.option("multiline", "true").json("March_19")
    val march2_19df = spark.read.option("multiline", "true").json("March2_19")

    val april1_19df = spark.read.option("multiline", "true").json("April_19")
    val april2_19df = spark.read.option("multiline", "true").json("April2_19")

    val may1_19df = spark.read.option("multiline", "true").json("May_19")
    val may2_19df = spark.read.option("multiline", "true").json("May2_19")

    val june1_19df = spark.read.option("multiline", "true").json("June_19")
    val june2_19df = spark.read.option("multiline", "true").json("June2_19")

    val july1_19df = spark.read.option("multiline", "true").json("July_19")
    val july2_19df = spark.read.option("multiline", "true").json("July2_19")

    val aug1_19df = spark.read.option("multiline", "true").json("Aug_19")
    val aug2_19df = spark.read.option("multiline", "true").json("Aug2_19")

    val sept1_19df = spark.read.option("multiline", "true").json("Sept_19")
    val sept2_19df = spark.read.option("multiline", "true").json("Sept2_19")

    val oct1_19df = spark.read.option("multiline", "true").json("Oct_19")
    val oct2_19df = spark.read.option("multiline", "true").json("Oct2_19")


    //------------------------------------2020-----------------------------------------

    val feb1_df = spark.read.option("multiline", "true").json("February_20")
    val feb2_df = spark.read.option("multiline", "true").json("February2_20")

    val march1_df = spark.read.option("multiline", "true").json("March_20")
    val march2_df = spark.read.option("multiline", "true").json("March2_20")

    val april1_df = spark.read.option("multiline", "true").json("April_20")
    val april2_df = spark.read.option("multiline", "true").json("April2_20")

    val may1_df = spark.read.option("multiline", "true").json("May_20")
    val may2_df = spark.read.option("multiline", "true").json("May2_20")

    val june1_df = spark.read.option("multiline", "true").json("June_20")
    val june2_df = spark.read.option("multiline", "true").json("June2_20")

    val july1_df = spark.read.option("multiline", "true").json("July_20")
    val july2_df = spark.read.option("multiline", "true").json("July2_20")

    val aug1_df = spark.read.option("multiline", "true").json("August_20")
    val aug2_df = spark.read.option("multiline", "true").json("August2_20")

    val sept1_df = spark.read.option("multiline", "true").json("September_20")
    val sept2_df = spark.read.option("multiline", "true").json("September2_20")

    val oct1_df = spark.read.option("multiline", "true").json("Oct_20")

    //feb_week1_df.printSchema()
    //feb1_df.select(functions.explode(feb1_df("results"))).select("col.created_at","col.text", "col.retweet_count",
    //  "col.favorite_count", "col.reply_count").show()

    flattenMonth(feb1_df, "Feb(1)")
    flattenMonth(feb2_df, "Feb(2)")
    flattenMonth(march1_df, "March(1)")
    flattenMonth(march2_df, "March(2)")
    flattenMonth(april1_df, "April(1)")
    flattenMonth(april2_df, "April(2)")
    flattenMonth(may1_df, "May(1)")
    flattenMonth(may2_df, "May(2)")
    flattenMonth(june1_df, "June(1)")
    flattenMonth(june2_df, "June(2)")
    flattenMonth(july1_df, "July(1)")
    flattenMonth(july2_df, "July(2)")
    flattenMonth(aug1_df, "Aug(1)")
    flattenMonth(aug2_df, "Aug(2)")
    flattenMonth(sept1_df, "Sept(1)")
    flattenMonth(sept2_df, "Sept(2)")
    flattenMonth(oct1_df, "Oct(1)")

    flattenMonth(feb1_19df, "Feb(1) 2019")
    flattenMonth(feb2_19df, "Feb(2) 2019")
    flattenMonth(march1_19df, "March(1) 2019")
    flattenMonth(march2_19df, "March(2) 2019")
    flattenMonth(april1_19df, "April(1) 2019")
    flattenMonth(april2_19df, "April(2) 2019")
    flattenMonth(may1_19df, "May(1) 2019")
    flattenMonth(may2_19df, "May(2) 2019")
    flattenMonth(june1_19df, "June(1) 2019")
    flattenMonth(june2_19df, "June(2) 2019")
    flattenMonth(july1_19df, "July(1) 2019")
    flattenMonth(july2_19df, "July(2) 2019")
    flattenMonth(aug1_19df, "Aug(1) 2019")
    flattenMonth(aug2_19df, "Aug(2) 2019")
    flattenMonth(sept1_19df, "Sept(1) 2019")
    flattenMonth(sept2_19df, "Sept(2) 2019")
    flattenMonth(oct1_19df, "Oct(1) 2019")


  }

  def flattenMonth(df: DataFrame, month: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    var month_flat = df.select(functions.explode(df("results"))).select("col.created_at", "col.text", "col.retweet_count",
      "col.favorite_count", "col.reply_count")


    month_flat.select(avg($"retweet_count").as(s"${month} Retweet avg")).show()
    month_flat.select(avg($"favorite_count").as(s"${month} Likes avg")).show()
    month_flat.select(avg($"reply_count").as(s"${month} Comments avg")).show()

  }
}
