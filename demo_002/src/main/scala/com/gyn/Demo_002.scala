package com.gyn

import org.apache.spark.sql.SparkSession

class Demo_002 {

}

object Demo_002 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("demo_001").getOrCreate()
    val aLines = spark.read.textFile("demo_002/input/a.txt").rdd
    val bLines = spark.read.textFile("demo_002/input/b.txt").rdd

    aLines.subtract(bLines).map {
      x =>
        val msg = x.split("\\s+")
        msg(0) + " " + msg(1)
    }.saveAsTextFile("demo_002/output/result")

  }

}
