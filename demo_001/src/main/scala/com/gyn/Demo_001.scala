package com.gyn

import org.apache.spark.sql.SparkSession

class Demo_001 {

}

object Demo_001 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("demo_001").getOrCreate()
    val lines = spark.read.textFile("demo_001/input/example.txt").rdd
    val name = "zhang san"

    val zhansanRdd = lines.filter(x => x.contains(name))

    zhansanRdd.saveAsTextFile("demo_001/output/zhangsan")

  }
}
