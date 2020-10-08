package com.siwoo.structure

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object SqlFunctionTutorial {
    val spark = SparkSession.builder
            .appName("sqlfunctiontutorial")
            .master("local")
            .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("warn")

    val MOUNT_PATH = SqlFunctionTutorial.getClass.getClassLoader.getResource("./")

    def main(args: Array[String]): Unit = {
        val df = spark.read.option("inferSchema", true)
                .option("header", true)
                .csv(s"$MOUNT_PATH/retail-data/by-day/*.csv")
        val SHOW_NUM = 5
        df.printSchema
        df.cache()

        //spark data type
        {
            //literal
            spark.range(2)
                    .select(lit(5), lit("five"), lit(5.0))
                    .show()

            //boolean
            df.withColumn("condition", $"invoiceno" === 536365)
                    .where($"condition")
                    .select($"invoiceno", $"description")
                    .show(SHOW_NUM)

            //boolean - and , or
            val orPredicate = ($"unitprice" > 600).or($"description".contains("POSTAGE"))   //or

            //df.select($"stockcode").distinct().show(SHOW_NUM)

            df.where($"stockcode".isin("DOT", "90210B")) //and
                    .where(orPredicate)
                    .select($"invoiceno", $"unitprice", $"description", $"stockcode")
                    .show(SHOW_NUM)

            //boolean filter with columns
            df.withColumn("condition",
                $"stockcode".isin("DOT", "90210B").and(
                ($"unitprice" > 600).or($"description".contains("POSTAGE"))))
                    .where($"condition")
                    .select($"invoiceno", $"unitprice", $"description", $"stockcode")
                    .show(SHOW_NUM)
        }

        //numeric
        {
            //pow
            df.select($"invoiceno", $"quantity", (pow($"unitprice" * $"quantity", 2) + 5).as("realQuantity"))
                    .show(SHOW_NUM)

            //round(col, 소숫점자리)
            df.select(round(pow($"unitprice" * $"quantity", 2)+ 5, 2).as("readQuantity"))
                    .show(SHOW_NUM)

            //corr (상관계수)
            df.select(corr($"unitprice", $"quantity")).show

            //집계 요약.
            df.describe("unitprice", "quantity", "country").show()

            //unique id for row
            df.select((monotonically_increasing_id() + 1).as("rowid"), expr("*")).show(SHOW_NUM)
        }

        //strings
        {
            //첫문자 대문자화
            df.select(
                initcap($"description")
            ).show(SHOW_NUM)

            //lower, upper
            df.select(lower($"description"), upper($"description"))
                    .show(SHOW_NUM)

            //trim, pad
            spark.range(1).select(lit(" h ").as("col"))
                    .select(
                        $"col",
                        ltrim($"col").as("ltrim"),
                        rtrim($"col").as("rtrim"),
                        trim($"col").as("trim"),
                        lpad(trim($"col"), 5, "*").as("lpad"),
                        rpad(trim($"col"), 5, "*").as("lpad")
                    ).show()

            //regex
            val colors = Seq("white", "red", "black", "green", "blue")
            val regex = colors.map(_.toUpperCase).mkString("|")

            df.select($"description", regexp_replace($"description", regex, "*").as("replace"))
                    .show(SHOW_NUM)

            //translate (index-level replacement)
            df.select($"description", translate(upper($"description"), "abcdef", "!@#$%^").as("translated"))
                    .show(SHOW_NUM)

            //regex with group 0 - whole group, 1...x group index
            val regexGroup = colors.map(_.toUpperCase).mkString("(", "|", ")")
            df.select($"description", regexp_extract($"description", regexGroup, 1).as("first_occr"))
                    .where(length($"first_occr") =!= 0)
                    .show(SHOW_NUM)

            //contains with varargs
            val columns = colors.map(c => $"description".contains(c.toUpperCase).as(s"is_$c")) :+ expr("*")
            df.select(columns:_*).where($"is_white").show(SHOW_NUM)
        }

        //date
        {
            //current
            val dateDF = spark.range(1)
                    .select(current_date().as("today"), current_timestamp().as("now"))

            //add & sub
            dateDF.select(
                expr("*"),
                date_add($"today", 5),
                date_sub($"today", 5)
            ).show()

            //diff & months_between & to_date
            dateDF.withColumn("start", to_date(lit("2020-01-01"), "yyyy-MM-dd"))
                    .withColumn("end", to_date(lit("2020-12-31"), "yyyy-MM-dd"))
                    .select(
                        expr("*"),
                        datediff($"end", $"start"),
                        months_between($"end", $"start")
                    ).show()
        }

        //null
        {
            val schema = StructType(Seq(
                StructField("a", IntegerType, true),
                StructField("b", IntegerType, true)
            ))

            val rdd = spark.sparkContext.parallelize(Seq((1, 1), (1, null), (null, 1), (null, null)).map(t => Row(t._1, t._2)))
            val nullableDF = spark.createDataFrame(rdd, schema)
            nullableDF.show()

            //coalesce (default value for null)
            nullableDF.select(expr("*"), coalesce($"a", lit(0)), coalesce($"a", $"b"))
                    .show()

            //drop nulls
            //drop rows if any of columns is null
            nullableDF.na.drop("any", Seq("a", "b")).show()
            //drop rows if all columns are null
            nullableDF.na.drop("all", Seq("a", "b")).show()

            //fill nulls
            nullableDF.na.fill(Map("a" -> 0, "b" -> 9999)).show()
        }
    }
}
