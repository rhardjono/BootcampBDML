package com.rhardjono.keepcoding.domain

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.rhardjono.keepcoding.AppConfig
import org.apache.spark.util.LongAccumulator


case class Geolocation(latitude: Double,
                       longitude: Double,
                       city: String,
                       country: String)

/*
  Se ha considerado el campo "Account_Created" del dataset como identificador de una cuenta bancaria.
  Como se ha definido de forma secuencial, tendremos que no puede haber mas de una transaccion por cuenta y cliente, es decir,
  cada transaccion de un mismo cliente se habra realizado desde una cuenta distina.
  */
case class Transaction(transactionOrder: Long,
                       transactionDate: Timestamp,
                       name: String,
                       price: Double,
                       description: String,
                       category: String,
                       paymentType: String,
                       geolocation: Geolocation)

case class Customer(name: String,
                   account: String,
                   product: String)


object Transaction {

  val sdf: SimpleDateFormat = new SimpleDateFormat("dd/MM/yy HH:mm")

  def apply(line: String, acc: LongAccumulator, metrics: Metrics, config: AppConfig): Transaction = {

    val fields = line.split(",")
    acc.add(1)

    Transaction(acc.value,
      new Timestamp(sdf.parse(fields(0).replace("[", "")).getTime()),
      fields(4).trim.toUpperCase,
      fields(2).toDouble,
      fields(10).replace("]", "").trim,
      metrics.getCategory(fields(10).replace("]", "").trim),
      fields(3).trim,
      Geolocation(fields(8).toDouble, fields(9).toDouble, fields(5).trim, metrics.getCountryByCoordinates(fields(8).toDouble, fields(9).toDouble, config.envOrElseConfig("google.apiKey"))))
  }


}