package main

import data_cap.data


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object main {
  val appName = "Tester"
  val master= "local[4]"
  val conf = new SparkConf().setAppName(appName).setMaster(master)
  val sc = new SparkContext(conf)

  val spark = SparkSession.builder()
    .appName("Lakers Data")
    .master("local[4]")
    .getOrCreate()


  spark.sparkContext.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    val dr= new data()

    val year= List[Int](2019,2020)
    val month = List[String]("January","February","March","April","May","June","July","August","September","October")

    for (i<-0 to year.length-1){
      for (j<-0 to month.length-1){
        println(s"${year(i)}-${month(j)}")
        dr.dataCollector(spark,year(i).toString,month(j))
        dr.dataReader(spark,s"Extracted-Data/${year(i)}-${month(j)}",month(j),year(i).toString)
      }
    }

    // dr.dataCollector(spark,year="2020",month="February")




  }

}
