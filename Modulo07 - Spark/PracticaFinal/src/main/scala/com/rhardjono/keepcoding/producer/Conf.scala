package com.rhardjono.keepcoding.producer

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

case class ApplicationConfig(configPath: String) {
  val config: Config = ConfigFactory.parseFile(new File(configPath))
  val broker: String = config.getString("kafka.broker")
  val port: Int = config.getInt("kafka.port")
  val topic: String = config.getString("kafka.topic")
  val delay: String = config.getString("kafka.delay")
  val numberOfMessages: String = config.getString("kafka.numberOfMessages")
}
