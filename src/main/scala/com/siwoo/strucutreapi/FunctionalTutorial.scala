package com.siwoo.strucutreapi

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object FunctionalTutorial {
    val spark = SparkSession.builder
            .master("local[*]")
            .appName("functionaltutorial")
            .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("warn")
    val MOUNT_PATH = FunctionalTutorial.getClass.getClassLoader.getResource("./").getFile

    def main(args: Array[String]): Unit = {
        val df = spark.read
                .option("header", true)
                .option("inferSchema", true)
                .csv(s"$MOUNT_PATH/retail-data/by-day/*.csv")

        df.printSchema()
        df.createOrReplaceTempView("retail")

        //spark internal type
        {
            //constant
            spark.range(5)
                .select(lit(5), lit("five"), lit(5.0))
                .show()

            //boolean
            df.withColumn("condition", $"invoiceno" === 536365)
                    .where($"condition")
                    .show(5)

            //and & or
            val pricePredicate = $"unitprice" > 600
            val descPredicate = $"description".contains("POSTAGE")
            val predicate = pricePredicate.or(descPredicate)    //or
            
            df.where(predicate)
                .where($"stockcode".isin("DOT", "85123A"))
                .show(5)

            //and & or with columns
            df.withColumn("condition",
                (($"unitprice" > 600).or($"description".contains("WHITE")))
                        .and($"stockcode".isin("DOT", "85123A")))
                    .where($"condition")
                    .show(5)
            
            //not
            df.withColumn("isExpensive", not($"unitprice" < 250))
                    .where($"isExpensive")
                    .show(5)

            //numeric
            val realQuantity = pow($"unitprice" * $"quantity", 2) + 5
            df.select(realQuantity.as("readQuantity"))
                    .show(5)

            //rounding
            df.select($"customerid", (round(pow($"unitprice" * $"quantity", 1) + 5, 1)).as("readQuantity"))
                    .show(5)

            //aggregation
            df.describe("unitprice", "quantity").show(5)

            //adding incrementing id
            df.select(monotonically_increasing_id().as("rownum"), expr("*")).show(5)
        }

        //strings
        {
            //case
            df.select(
                initcap($"description"),
                upper($"description"),
                lower($"description")
            ).show(5)

            //formatting
            spark.range(1)
                    .select(
                        lit(" hello ").as("s1"),
                        lit("pad").as("s2"))
                    .select(
                        expr("*"),
                        ltrim($"s1").as("ltrim"),
                        rtrim($"s1").as("rtrim"),
                        trim($"s1").as("trim"),
                        lpad($"s2", 5, "*"),
                        rpad($"s2", 5, "*")
                    ).show()

            //regex
            val colors = Seq("black", "white", "red", "green", "blue")
            val regex = colors.map(_.toUpperCase).mkString("|")

            df.select($"description", regexp_replace($"description", regex, "***").as("hasColor"))
                    .show(5)

            val regexGroup = colors.map(_.toUpperCase()).mkString("(", "|", ")")
            df.select($"description", regexp_extract($"description", regexGroup, 1).as("first_occurrence"))
                    .show(5)

            //translate - index level replacement
            df.select(translate(lower($"description"), "abcdef", "!@#$%^"))
                    .show(5)

            // varargs + string search
            val columns = colors.map(c => $"description".contains(c.toUpperCase()).as(s"is_$c")) :+ expr("*")
            df.select(columns:_*)
                    .where($"is_white".or($"is_black"))
                    .show(5)
        }

        //Date & Timestamp
        {
            //current time
            val dateDF = spark.range(1)
                    .select(current_date().as("today"), current_timestamp().as("now"))
            dateDF.show()

            //adding & sub time
            dateDF.withColumn("yesterday", date_sub($"today", 1))
                    .withColumn("tomorrow", date_add($"today", 1))
                    .show()

            //parsing date & date diff
            dateDF.withColumn("go", to_date(lit("1989-03-04"), "yyyy-MM-dd"))
                    .select(
                        datediff($"today", $"go"),
                        round(months_between($"today", $"go")))
                    .show()
        }

        //null
        {
            val schema = StructType(Seq(
                StructField("one", IntegerType, true),
                StructField("two", IntegerType, true)
            ))

            val rows = Seq((1, 1), (1, null), (null, 1), (null, null)).map(pair => Row(pair._1, pair._2))
            val df2 = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

            //default if null
            df2.select(coalesce($"one", lit(999)), coalesce($"two", $"one")).show()

            df2.na.drop("any").show()  //any columns that contains null
            df2.na.drop("all")  //all columns that contains only null
            df2.na.drop("any", Seq("two"))  //if column 'two' is null then drop

            df2.na.fill(Map("one" -> -1, "two" -> -2)).show()
        }
    }
}
