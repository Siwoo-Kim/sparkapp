package com.siwoo.structure

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object JsonTutorial {
    val spark = SparkSession.builder
            .appName("jsontutorial")
            .master("local")
            .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("warn")
    val om = new ObjectMapper
    om.registerModule(DefaultScalaModule)

    case class Json(key: String, values: Seq[Int])

    def main(args: Array[String]): Unit = {
        val jsons = Seq(
            Json("key1", Seq(1, 2, 3, 4)),
            Json("key1", Seq(1, 2, 4, 5)),
            Json("key1", Seq(4, 3, 2, 1)))
        val jsonStrings = jsons.map(j => om.writeValueAsString(j))

        val df = jsonStrings.toDF("json")
        df.show()

        //parse (json -> object)
        {
            //querying
            df.select($"json",
                get_json_object($"json", "$").as("root"),
                get_json_object($"json", "$.key").as("path1"),
                get_json_object($"json", "$.values[0]").as("path2")
            ).show()

        }

        //serialize (object -> json)
        {
            val df2 = jsons.toDF("key", "values")
            df2.show()

        }
    }

}
