package com.gyn

import org.apache.spark.sql.SparkSession

class Demo_006 {

}

object Demo_006 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("demo_001").getOrCreate()
    val persons = spark.read.textFile("demo_006/input/example.txt").rdd
    persons.groupBy {
      x =>
        val word = x.split("\\s+")
        word(1)
    }.map {
      x =>
        (x._1, x._2.toList)
    }.saveAsTextFile("demo_006/output")
  }
}


