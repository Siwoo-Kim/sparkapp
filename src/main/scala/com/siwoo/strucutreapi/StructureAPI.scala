package com.siwoo.strucutreapi

import org.apache.spark.sql.types.{BooleanType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object StructureAPI {

    val spark = SparkSession.builder()
            .appName("structureapi")
            .master("local")
            .getOrCreate()
    import spark.implicits._

    def main(args: Array[String]): Unit = {
        //column
        val column = $"num"
        var df = spark.range(100).toDF("num").select(column)

        //row
        val rows: Array[Row] = df.take(10)
        rows.foreach(r => println(r.getAs[Int]("num")))

        val schema = StructType(
            Seq(StructField("num", IntegerType, false),
                StructField("even", BooleanType, false))
        )
    }
}
