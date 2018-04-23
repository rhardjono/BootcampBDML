package com.rhardjono.keepcoding.producer

import org.joda.time.DateTime

import scala.util.{Failure, Success, Try}

  class MessageGenerator(applicationConf: String) {

    private val conf = ApplicationConfig(applicationConf)

    def produceMessages(): Unit = {

      val producer: KProducer[String, String] = new KProducer[String, String](conf)

      for (a <- 1 to conf.numberOfMessages.toInt) {
        val timestamp = DateTime.now().getMillis

        Thread.sleep(conf.delay.toLong)

        Try(producer.produce(conf.topic, "1", GenerateRandomData.getRandomTransaction)
        ) match {
          case Success(m) =>

            val metadata = m.get()
            println("Success writing to Kafka topic:" + metadata.topic(),
              metadata.offset(),
              metadata.partition(),
              new DateTime(metadata.timestamp()))
          case Failure(f) => println("Failed writing to Kafka", f.printStackTrace())
        }
      }
    }


  }