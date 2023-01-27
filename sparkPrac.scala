package com.sundogsoftware.spark
import org.apache.avro.Schema
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.functions._

object SparkPrac {
  def main(args: Array[String]):Unit={
    val spark = SparkSession.builder.appName( name = "Validation").master( master = "Local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.format("avro").load ("<path>")
    val tdf = spark.read.format ("avro").load ("<path>")
    //similar to sql query for inner join on column record_id
    val tdf1 = df.join(tdf, df("record_id") === tdf("recordId"), "inner")
    //filter rows with regex expression
    val df_regex = df.filter(col("F5").rlike("^[A-Za-20-9!\"#$%&'()*+,-./:; ]*$"))
    //get record ID's which are duplicate
    val rdf = df.groupBy("recordId").agg(count("recordId").alias("count")).filter(col("count")>1)
    //get duplicate records with filter on specific column
    val rrdf = spark.sql("sql(\"Select recordId, count(*) from snowflake where <col_name> = '<value>' group by recordId,track_id having count (*) > 1")
  }
}
