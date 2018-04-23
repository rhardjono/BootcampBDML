package com.rhardjono.keepcoding

import com.rhardjono.keepcoding.batchlayer.{MetricsSQL, TestBatchSQL}

object BatchApp {

  def main(args: Array[String]): Unit = {

    if (args.length == 2) {
      new MetricsSQL().run(args)
      //TestBatchSQL.run(args)
    } else {
      System.err.println(s"""
           |Running Spark Job with wrong parameters.
           |
           |Usage: StreamingAPP path_dataset path_output
         """.stripMargin)
      System.exit(1)
    }
  }

}
