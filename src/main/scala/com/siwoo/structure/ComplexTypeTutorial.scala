package com.siwoo.structure

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypeTutorial {
    val spark = SparkSession.builder
            .appName("complextypetutorial")
            .master("local")
            .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("warn")
    val MOUNT_PATH = ComplexTypeTutorial.getClass.getClassLoader.getResource("./")

    def main(args: Array[String]): Unit = {
        val df = spark.read
                .option("inferSchema", true)
                .option("header", true)
                .csv(s"$MOUNT_PATH/retail-data/by-day/2010-12-*.csv")
        df.cache()
        val SHOW_NUM = 5

        //struct
        {
            //creating struct
            df.select($"invoiceno", $"description",
                struct($"invoiceno", $"description").as("struct"))
                    .show(SHOW_NUM)

            //accessing properties from struct
            df.select(struct($"invoiceno", $"description").as("struct"))
                    .select(
                        $"struct",
                        $"struct.invoiceno",
                        $"struct.description",
                        $"struct.*" //root 레벨로 프로퍼터 끌어올리기
                    ).show(SHOW_NUM)
        }

        //array
        {
            //creating array using split
            df.select($"description", split($"description", " ").as("array"))
                    .show(SHOW_NUM)

            //accessing array element by index
            df.withColumn("array", split($"description", " "))
                    .select(expr("array[0]"), expr("array[1]"))
                    .show(SHOW_NUM)

            //length of array
            df.withColumn("array", split($"description", " "))
                    .select(size($"array"))
                    .show(SHOW_NUM)

            //is e in the array ?
            df.withColumn("array", split($"description", " "))
                    .where(array_contains($"array", "WHITE"))
                    .show(SHOW_NUM)

            //explode
           val df2 = Seq(("a b z", "b c", "e f")).toDF("c1", "c2", "c3")
           df2.withColumn("array", split($"c1", " "))
                   .select(expr("*"), explode($"array"))
                   .show()
        }

        //map
        {
            //creating map
            df.select($"description", $"invoiceno", map($"description", $"invoiceno").as("map"))
                    .show(SHOW_NUM)

            //accessing value by key from map
            df.select(map($"description", $"invoiceno").as("map"))
                    .select(expr("map['WHITE METAL LANTERN']").as("accessMap"))
                    .where("accessMap is not null")
                    .show(SHOW_NUM)
        }
    }
}
