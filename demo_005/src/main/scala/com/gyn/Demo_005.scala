package com.gyn

import org.apache.spark.sql.SparkSession

class Demo_005 {

}

object Demo_005 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("demo_001").getOrCreate()
    val persons = spark.read.textFile("demo_005/input/example.txt").rdd
    persons.filter {
      x =>
        val msg = x.split("\\s+")
        msg(1).toInt < 20
    }.saveAsTextFile("demo_005/output")
  }
}
