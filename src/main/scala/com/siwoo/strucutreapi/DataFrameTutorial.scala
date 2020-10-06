package com.siwoo.strucutreapi

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{avg, countDistinct, expr, lit, lower}
import org.apache.spark.sql.types._

object DataFrameTutorial {
    val spark = SparkSession.builder()
            .appName("dataframeapp")
            .master("local[*]")
            .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("warn")

    val MOUNT_PATH = DataFrameTutorial.getClass.getClassLoader.getResource("./")

    def main(args: Array[String]): Unit = {
        //manual schema from StructType
        val mySchema = StructType(Seq(
            StructField("DEST_COUNTRY_NAME", StringType, false),
            StructField("ORIGIN_COUNTRY_NAME", StringType, false),
            StructField("count", LongType, false, Metadata.fromJson("{\"hello\":\"meta\"}"))
        ))

        //create df from datasource
        var df = spark.read
                .schema(mySchema)
                .json(s"$MOUNT_PATH/flight-data/json/*.json")
        df.cache()
        df.show(2)

        //printing schema
        df.printSchema()

        //column example
        {
            val column = $"dest_country_name"
            val explicitColumn = df.col("dest_country_name")
            df.select(explicitColumn, column).show(2)

            for (c <- df.columns)
                println(c)
        }

        //expression example
        {
            val exp1 = ((($"some") + 5) * 200) < $"other"
            val exp2 = expr("(((some) + 5) * 200) < other")
            println(exp1.toString() == exp2.toString())
        }

        //rows
        {
            val firstRow: Row = df.first()
            println(firstRow.getAs[String]("DEST_COUNTRY_NAME"))
            println(firstRow.getAs[String](1))
            println(firstRow.getAs[Long]("count"))

            val schema2 = StructType(Seq(
                StructField("from", StringType, false),
                StructField("to", StringType, false),
                StructField("edge", StringType, false)
            ))
            val rows = Seq(Row("v1", "v2", "v1 -> v2"), Row("v2", "v3", "v2 -> v3"))
            val rdd = spark.sparkContext.parallelize(rows)
            spark.createDataFrame(rdd, schema2).show()
        }

        /**
         * 4 types of transformation
         *  1. adding rows and columns (union, withColumn)
         *  2. deleting rows and columns (drop, delete)
         *  3. changing rows or columns (cast, map, where, withColumnRenamed, select ...)
         *  4. changing order of rows (partitioning, orderBy)
         */
        df.createOrReplaceTempView("flights")

        //select example
        {
            df.select($"dest_country_name", $"origin_country_name").show(2)

            //alias & expr & adding column
            df = df.select(
                expr("dest_country_name as destination"),
                expr("origin_country_name as origin"),
                expr("(dest_country_name = origin_country_name) as within"),
                $"count")
            df.show()

            //aggregation on select
            df.select(avg($"count"), countDistinct($"destination")).show()

            //literal
            df.select(expr("*"), lit(1).as("one")).show()
        }

        //drop columns
        df = df.drop("within")
        df.printSchema()

        //adding & renaming columns
        {
            df = df.withColumn("within", $"origin" === $"destination")
            for (c <- df.columns)
                println(c)
            df = df.withColumnRenamed("count", "counts")
        }

        //casting
        {
            df.withColumn("countString", $"counts".cast("string"))
                    .show()
        }

        //filtering
        {
            df.where(($"counts" > 10).or($"counts" === 5))  //or
                    .where(lower($"origin") =!= "croatia")  //and (default when combining filters)
                    .orderBy($"counts".asc_nulls_last)
                    .show()
        }

        df.createOrReplaceTempView("flights")

        //distinct rows && distinct count rows (aggregation)
        {
            df.select($"destination", $"origin").distinct().show()
            spark.sql("select count(distinct(destination, origin)) from flights").show()
        }

        //sampling
        {
            val withReplacement = false;
            val fraction = 0.5  //0.1 - 1.0
            df.sample(withReplacement, fraction).show()
        }

        //adding & union rows
        {
            val newDF = Seq(("Korea", "Canada", 20l, false), ("Canada", "Korea", 24l, false))
                    .toDF("destination", "origin", "counts", "arbitaryname")
                    //union is based on the column index, so the column name doesn't matter
            df.union(newDF).where($"destination" === "Korea")
                    .show()
        }

        //sorting & limiting
        {
            df.sort($"counts".desc_nulls_last)
                    .limit(5)
                    .show()
        }

        //repartitioning & coalesce
        {
            df.repartition( $"destination") //partitioning by column for optimizing query
                    .coalesce(1)    //coalescing partitions so that produce 1 file
                    .write
                    .mode("overwrite")
                    .parquet(s"$MOUNT_PATH/save")

            spark.read.parquet(s"$MOUNT_PATH/save")
                    .select($"destination").show(5)
        }
    }

}
