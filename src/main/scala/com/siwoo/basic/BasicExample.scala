package com.siwoo.basic

import java.util.Scanner

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object BasicExample {
    val log = LoggerFactory.getLogger(BasicExample.getClass)

    val scanner = new Scanner(System.in)
    val spark = SparkSession.builder()
            .appName("transformation")
            .master("local[*]")
            .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", 5)
    spark.sparkContext.setLogLevel("warn")
    import spark.implicits._

    def main(args: Array[String]): Unit = {
        val MOUNT_PATH = if (args == null)
            BasicExample.getClass.getClassLoader.getResource("./flight-data/csv").getPath
            else args(0)

        val df = spark.read.option("inferSchema", true)
                .option("header", true)
                .csv(s"$MOUNT_PATH/flight-data/csv/*.csv")

        //sql way
        df.createOrReplaceTempView("flights")
        val sqlway = spark.sql("""
              select dest_country_name, count(1) as counts
              from flights
              group by dest_country_name
              order by counts desc
            """)

        //df way
        val dfway = df.groupBy("dest_country_name")
                .agg(count($"dest_country_name").as("counts"))
                .orderBy($"counts".desc_nulls_last)

        // compare execution plan
        sqlway.explain()
        dfway.explain()

        //spark provides functions
        df.select(max($"count")).show()

        //spark provides aggregation
        spark.sql("""
              select dest_country_name, sum(count) as sums
              from flights
              group by dest_country_name
              order by sums desc
              """).show(5)

        val df2 = df.groupBy("dest_country_name")
                .agg(sum($"count").as("sums"))
                .orderBy($"sums".desc_nulls_last)

        df2.explain()
        df2.show()
        while (true) {
            val line = scanner.nextLine()
            if (":q".eq(line))
                return
        }
    }

}
