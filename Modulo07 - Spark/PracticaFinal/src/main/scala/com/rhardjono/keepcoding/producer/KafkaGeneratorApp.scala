package com.rhardjono.keepcoding.producer

object KafkaGeneratorApp extends App {

  println("~~~~~~~~~~~~~~~~~~~")
  println("  Kafka Generator  ")
  println("~~~~~~~~~~~~~~~~~~~")

  if (args.length == 1){
    new MessageGenerator(args(0)).produceMessages()
  }else {
    println("Usage: java -jar PracticaFinal-KafkaProducer-1.0-SNAPSHOT.jar <application.conf>")
  }
}
