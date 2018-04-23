package com.rhardjono.keepcoding.speedlayer

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties

import com.rhardjono.keepcoding.AppConfig
import com.rhardjono.keepcoding.domain._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferBrokers
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.LongAccumulator

object MetricsStreaming {

  private val metricConf = new AppConfig()

  def getKafkaDefaultConfig(): Properties = {
    val prop = new Properties()
    prop.put("bootstrap.servers", "localhost:9092")
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop
  }

  def getKafkaCustomConfig(): Properties = {
    val prop = new Properties()
    prop.put("bootstrap.servers", "localhost:9092")
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "com.rhardjono.keepcoding.domain.serde.TransactionSerializer")
    prop
  }

  def writeStringToKafka(outputTopic: String, config: Properties)(partitionOfRecords: Iterator[Row]): Unit = {
    val producer = new KafkaProducer[String, String](config)
    partitionOfRecords.foreach(data => producer.send(new ProducerRecord[String, String](outputTopic, data.toString())))
    producer.flush()
    producer.close()
  }

  def writeTransactionToKafka(outputTopic: String, config: Properties)(partitionOfRecords: Iterator[Transaction]): Unit = {
    val producer = new KafkaProducer[String, Transaction](config)
    partitionOfRecords.foreach(data => producer.send(new ProducerRecord[String, Transaction](outputTopic, null, data)))
    producer.flush()
    producer.close()
  }

  def run(args: Array[String]): Unit = {

    // Create Spark Session
    val session = SparkSession
      .builder()
      .master("local[1]")
      .appName("Final Project - Speed Layer")
      .getOrCreate()

    import session.implicits._

    /* TOPICS */
    //consoleTransaction randomTransaction metric1 metric2 metric3 metric4 metric5 metric6 metric7 metricTest
    val List(topicConsole, topicRandom, topic1, topic2, topic3, topic4, topic5, topic6, topic7, topicTest) = args.toList

    // Create Streaming Context and Kafka Direct Stream
    val ssc = new StreamingContext(session.sparkContext, Seconds(30))
    ssc.sparkContext.setLogLevel("WARN")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "transaction",
      "kafka.consumer.id" -> "kafka-consumer-01"
    )

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc, PreferConsistent, Subscribe[String, String](Array(topicConsole, topicRandom), kafkaParams)
    )

    /* Print kafka stream (ke, value) */
    //val dataStreamKV: DStream[(String, String)] = kafkaDStream.map(record => (record.key(), record.value()))
    //dataStreamKV.print()

    val dataStream: DStream[String] = kafkaDStream.map(record => record.value())

    val transactionAcc: LongAccumulator = ssc.sparkContext.longAccumulator("idTransaction")
    val sdf: SimpleDateFormat = new SimpleDateFormat("dd/MM/yy HH:mm")
    val metrics = new Metrics(session)

    val parsedDataStream = dataStream.map(_.split(",")).map(event => {
      transactionAcc.add(1)
      new Tuple2(Customer(event(4).trim.toUpperCase, event(6), event(1)),
        Transaction(
          transactionAcc.value,
          new Timestamp(sdf.parse(event(0).replace("[", "")).getTime()),
          event(4).trim.toUpperCase,
          event(2).toDouble,
          event(10).replace("]", "").trim,
          metrics.getCategory(event(10).replace("]", "").trim),
          event(3).trim,
          Geolocation(event(8).toDouble, event(9).toDouble, event(5).trim, metrics.getCountryByCoordinates(event(8).toDouble, event(9).toDouble, metricConf.envOrElseConfig("google.apiKey"))))
      )
    })
    //parsedDataStream.print()

    parsedDataStream.foreachRDD { rdd =>

      /* CUSTOMERS */
      val dStreamCustomers = rdd.map(_._1)
      dStreamCustomers.toDF().createOrReplaceGlobalTempView("CUSTOMERS")
      /* TRANSACTIONS */
      val dStreamTransactions = rdd.map(_._2)
      dStreamTransactions.toDF().createOrReplaceGlobalTempView("TRANSACTIONS")

      // #1
      val tareaSQL1 = metrics.metricaSQL1()
      println("METRICA#1: Transacciones por ciudad")
      tareaSQL1.show(false)
      tareaSQL1.rdd.foreachPartition(writeStringToKafka(topic1, getKafkaDefaultConfig))

      // #2
      val tareaSQL2 = metrics.metricaSQL2()
      println("METRICA#2: Clientes que han realizado pagos superiores a 3000 ")
      tareaSQL2.show(false)
      tareaSQL2.rdd.foreachPartition(writeStringToKafka(topic2, getKafkaDefaultConfig))

      // #3
      val tareaSQL3 = metrics.metricaSQL3()
      println("METRICA#3: Transacciones realizadas en la ciudad de New York ")
      tareaSQL3.show(false)
      tareaSQL3.rdd.foreachPartition(writeStringToKafka(topic3, getKafkaDefaultConfig))

      // #4
      val tareaSQL4 = metrics.metricaSQL4()
      println("METRICA#4: Transacciones cuya categoria sea Ocio")
      tareaSQL4.show(false)
      tareaSQL4.rdd.foreachPartition(writeStringToKafka(topic4, getKafkaDefaultConfig))

      // #5
      val tareaSQL5 = metrics.metricaSQL5()
      println("METRICA#5: Transacciones de cada cliente en los ultimos 30 dias")
      tareaSQL5.show(false)
      tareaSQL5.rdd.foreachPartition(writeStringToKafka(topic5, getKafkaDefaultConfig))

      // #6
      val tareaSQL6 = metrics.metricaSQL6()
      println("METRICA#6: Gasto medio por cliente en ocio en Reino Unido (UK)")
      tareaSQL6.show(false)
      tareaSQL6.rdd.foreachPartition(writeStringToKafka(topic6, getKafkaDefaultConfig))

      // #7
      val tareaSQL7 = metrics.metricaSQL7()
      println("METRICA#7: Clientes con un gasto en Seguros superior a 5000")
      tareaSQL7.show(false)
      tareaSQL7.rdd.foreachPartition(writeStringToKafka(topic7, getKafkaDefaultConfig))

    }

    ssc.start()
    ssc.awaitTermination()

  }


}

