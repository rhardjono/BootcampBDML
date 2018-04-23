package com.rhardjono.keepcoding

import com.rhardjono.keepcoding.speedlayer.{MetricsStreaming, MetricsStructuredStreaming}

object StreamingApp {
  def main(args: Array[String]): Unit={

    if(args.length == 10){
      MetricsStreaming.run(args)
      //MetricsStructuredStreaming.run(args)
    }else{
      System.err.println(s"""
           |Running Spark Job with wrong parameters.
           |
           |Usage: StreamingAPP console_transactions_topic random_transactions_topic {metric1_topic ... metric7_topic}
           |
         """.stripMargin)
      System.exit(1)
    }
  }
}
