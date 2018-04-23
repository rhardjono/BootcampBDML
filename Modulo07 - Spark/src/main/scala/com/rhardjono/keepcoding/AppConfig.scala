package com.rhardjono.keepcoding

import com.typesafe.config.ConfigFactory
import scala.util.Properties

class AppConfig(fileNameOption: Option[String] = None) extends Serializable {

  val config = fileNameOption.fold(
    ifEmpty = ConfigFactory.load() )(
    file => ConfigFactory.load(file) )

  def envOrElseConfig(name: String): String = {
    Properties.envOrElse(
      name.toUpperCase.replaceAll("""\.""", "_"),
      config.getString(name)
    )
  }

}