package com.siwoo.basic

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

/**
 * 트랜스포메이션
 * 분산 컬렉션을 변경하는 방법.
 * 액션이 처리하기 이전까지 호출되지 않음.
 * 스파크에 의해 실행 계획으로 표현됨.
 *
 * 트랜스포메이션의 종류
 *          1. 좁은 의존성
 *              - 1 partition produces 1 partition
 *                2. 넓은 의존성
 *              - shuffle 요구됨.
 *              - 1 partition produces more than 1 partition
 */
object Transformation {
    val spark = SparkSession.builder()
            .appName("transformation")
            .master("local[*]")
            .getOrCreate()
    spark.sparkContext.setLogLevel("warn")

    import spark.implicits._

    def main(args: Array[String]): Unit = {
        val df = spark.range(100).toDF("num")
        df.where("num % 2 == 0") //narrow transformation
                .explain()

        df.withColumn("even", expr("num % 2 == 0"))
                .groupBy($"even") //wide transformation
                .count()
                .show() // action
    }
}
