package com.rhardjono.keepcoding.producer

import java.util.Properties
import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

class KProducer[K <: String, V <: String](conf: ApplicationConfig) {
  val kafkaProps = new Properties()
  kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, s"${conf.broker}:${conf.port}")
  kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

  private lazy val producer = new KafkaProducer[String, String](kafkaProps)

  def produce(topic: String, key: String, value: String, partition: Int = 0) : Future[RecordMetadata] = {
    val record = new ProducerRecord(conf.topic, key, value)
    producer.send(record)
  }
}