package data_cap

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, explode, round}
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.apache.spark.{SparkConf, SparkContext}

class data {


  def dataCollector(spark:SparkSession, year: String, month:String): Unit ={
    val data = spark.read.option("multiline","true").json(s"Lakers-Twitter-Data/${year}/${month}")

    //data.select(explode($"results.created_at")).
    //val twitter_data = data.select(functions.explode(data("results"))).select("col.created_at", "col.text","col.retweet_count","col.favorite_count","col.reply_count")

    val twitter_data = data.select(functions.explode(data("results"))).select("col.retweet_count","col.favorite_count","col.reply_count")


    twitter_data.write
      .mode("overwrite")
      .option("header","true")
      .option("delimiter",",")
      .csv(s"Extracted-Data/${year}-${month}")

    //twitter_data.printSchema()

  }

  def dataReader(spark: SparkSession, path:String,month:String,year:String): Unit ={
    import spark.implicits._

    val c1=$"retweet_count"
    val c2=$"favorite_count"
    val c3=$"reply_count"


    val data= spark.read.option("header","true").csv(path)
      .withColumn(s"retweet_count",$"retweet_count".cast(IntegerType))
      .withColumn(s"favorite_count",$"favorite_count".cast(LongType))
      .withColumn(s"reply_count",$"reply_count".cast(LongType))
    data.show()
    //data.printSchema()

    val analytics= data
      .filter(c1>0)
      .filter(c2>0)
      .filter(c3>0)

    analytics

      .agg(functions.round(functions.avg(c1),2)
        .as(s"${year}-${month} Retweet avg"))
      .show()

    analytics
      .agg(functions.round(functions.avg(c2),2)
        .as(s"${year}-${month} Likes avg"))
      .show()

    analytics
      .agg(functions.round(functions.avg(c3),2)
        .as(s"${year}-${month} Replies avg"))
      .show()

    analytics
      .agg(functions.max(c1).as(s"${year}-${month} Retweet max"))
      .show()

    analytics
      .agg(functions.max(c2).as(s"${year}-${month} Likes max"))
      .show()

    analytics
      .agg(functions.max(c3).as(s"${year}-${month} Replies max"))
      .show()

    analytics
      .agg(functions.min(c1).as(s"${year}-${month} Retweet min"))
      .show()

    analytics
      .agg(functions.min(c2).as(s"${year}-${month} Likes min"))
      .show()

    analytics
      .agg(functions.min(c3).as(s"${year}-${month} Replies min"))
      .show()
  }


}
