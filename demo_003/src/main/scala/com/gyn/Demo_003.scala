package com.gyn

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class Demo_003 {

}

object Demo_003 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("demo_001").getOrCreate()
    val aLines = spark.read.textFile("demo_003/input/a.txt").rdd
    val bLines = spark.read.textFile("demo_003/input/b.txt").rdd

    val schemaString = "name ip age job"

    val fields = schemaString.split(" ").map(x => StructField(x,StringType,nullable = true))

    val schema = StructType(fields)

    val schema_b = StructType(List(
      StructField("name", StringType, nullable = false),
      StructField("country", StringType, nullable = true),
    ))


    val row = aLines.map(_.split("\\s+")).map(line => Row(line(0),line(1),line(2),line(3)))
    val row_b = bLines.map(_.split("\\s+")).map(line => Row(line(0),line(1)))

    val personDF = spark.createDataFrame(row,schema)
    val contryDF = spark.createDataFrame(row_b,schema_b)

    personDF.createOrReplaceTempView("person")
    contryDF.createOrReplaceTempView("country")

    val result = spark.sql("SELECT p.name,p.ip,p.age,p.job,c.country  FROM person  p join country  c where p.name = c.name")

    result.write.csv("demo_003/output/result")

  }
}
