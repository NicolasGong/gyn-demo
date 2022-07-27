package com.gyn

import org.apache.spark.sql.SparkSession

class Demo_004 {

}

object Demo_004 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("demo_001").getOrCreate()
    val persons = spark.read.textFile("demo_004/input/person.txt").rdd
    persons.map{
      x =>
        val msg = x.split("\\s+")
        (msg(1),1)
    }.reduceByKey(_ + _).saveAsTextFile("demo_004/output")

  }

}
