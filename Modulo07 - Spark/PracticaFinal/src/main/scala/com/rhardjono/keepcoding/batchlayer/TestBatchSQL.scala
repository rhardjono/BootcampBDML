package com.rhardjono.keepcoding.batchlayer

import com.rhardjono.keepcoding.domain.{Customer, Geolocation, Transaction}
import org.apache.spark.sql.SparkSession

object TestBatchSQL {

  def run(args: Array[String]): Unit = {

    val session = SparkSession.builder
      .master("local[*]")
      .appName("Final Project - Batch Layer")
      .getOrCreate()

    session.sparkContext.setLogLevel("WARN")

    import session.implicits._

    val rawData = session.read.csv(s"file:///${args(0)}")
    val header = rawData.first()
    val rawDataWithoutHeader = rawData.filter(!_.equals(header)).map(_.toString().split(","))

    /** Pair RDD **/
    val pair = rawDataWithoutHeader.map(record => (record(4), record))
    pair.show(truncate = false)
    println("============================")

    //
    val x = pair.rdd.groupByKey()
    // x.take(2).foreach(reg => reg._2.foreach(_.foreach(println(_))))
    for( reg <- x.take(2)){
      println( "Customer:" + reg._1 )
      for ( data <- reg._2){
        println("\tPayment Type:" + data(3))
      }
    }

    println("============================")
    val valores = x.values
    println("Transacciones:" + valores.count())

    println("============================")
    val y = x.map(pair => Customer("foo","account","product"))
    y.collect().foreach(println)

    println("============================")
    val z = x.flatMapValues(input => List((Customer("bar","account","product"))))
    z.collect().foreach(println)

  }

}

