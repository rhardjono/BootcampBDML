package com.rhardjono.keepcoding.speedlayer

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties

import com.rhardjono.keepcoding.AppConfig
import com.rhardjono.keepcoding.domain._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.LongAccumulator

object MetricsStructuredStreaming {

  def run(args: Array[String]): Unit = {

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Final Project - Speed Layer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val transactionAcc: LongAccumulator = spark.sparkContext.longAccumulator("idTransaction")
    val metrics = new Metrics(spark)
    val metricConf = new AppConfig()

    import spark.implicits._

    spark.udf.register("deserialize", (message: String) => Transaction(message, transactionAcc, metrics, metricConf))

    def createDataSet(spark: SparkSession): Dataset[Transaction] = {
      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "transactionsConsole")
        .option("startingOffsets", "latest")
        .load()
        .selectExpr("""deserialize(CAST(value as STRING)) AS value""")
        .select($"value".as[Transaction])
    }

    def startStreamingQueryFrom_Console[T](dataset: Dataset[T]): StreamingQuery = {
      dataset.writeStream
        .format("console")
        .outputMode("update")
        .option("truncate", "false")
        .start()
    }

    def startStreamingQueryFrom_Kafka[T](dataset: Dataset[T]): StreamingQuery = {
      dataset
       //.selectExpr("CAST(PRICE AS STRING) value")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "metricaTest")
        .option("checkpointLocation", "/home/keepcoding/KeepCoding/Workspace/PracticaFinal/src/main/checkpoint")
        .outputMode("complete")
        .start()
    }

    val ds = createDataSet(spark)


    val transactionsPerCustomer = ds.groupBy($"name").count()
    println(transactionsPerCustomer.isStreaming)
    transactionsPerCustomer.printSchema()

    val transactionsByCountryDescription = ds.groupBy($"geolocation.country", $"description").count()
    println(transactionsByCountryDescription.isStreaming)
    transactionsByCountryDescription.printSchema()


    val consoleQuery = startStreamingQueryFrom_Console(transactionsPerCustomer)
    val kafkaQuery = startStreamingQueryFrom_Kafka(transactionsByCountryDescription)

    Seq(consoleQuery, kafkaQuery).foreach(_.awaitTermination())

  }


}

