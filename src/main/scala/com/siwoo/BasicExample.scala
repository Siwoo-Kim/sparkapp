package com.siwoo

import java.util.Scanner

import org.apache.spark.sql.SparkSession

object BasicExample {
    val scanner = new Scanner(System.in)
    val spark = SparkSession.builder()
            .appName("transformation")
            .master("local[*]")
            .getOrCreate()
    spark.sparkContext.setLogLevel("warn")
    import spark.implicits._

    val awaitAnswer: () => Boolean = () => {
        while (true) {
            if (":q".equals(scanner.nextLine()))
                true
        }
        false
    }

    def main(args: Array[String]): Unit = {
        val MOUNT_PATH = BasicExample.getClass.getClassLoader.getResource("./flight-data/csv").getPath

        val df = spark.read.option("inferSchema", true)
                .option("header", true)
                .csv(s"$MOUNT_PATH/*.csv")

        df.take(3).foreach(println)
        if (awaitAnswer())
            return
    }

}
