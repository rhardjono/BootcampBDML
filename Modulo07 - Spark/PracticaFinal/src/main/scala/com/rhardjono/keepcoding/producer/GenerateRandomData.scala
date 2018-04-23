package com.rhardjono.keepcoding.producer

import java.sql.Timestamp
import java.text.SimpleDateFormat

import scala.language.postfixOps
import scala.util.Random


object GenerateRandomData {

  val sep = ","
  val paymentTypes = Seq("Visa", "Mastercard")
  val cities = Seq("Madrid", "Barcelona", "Paris", "Roma", "Sidney")
  val description = Seq("Cinema", "Sport", "car insurace", "home insurace", "insurance", "Shopping Mall", "Restaurant")


  def randomDate() : String = {
    val beginTime = Timestamp.valueOf("2000-01-01 00:00:00").getTime
    val endTime = Timestamp.valueOf("2018-12-31 00:58:00").getTime
    val diff = endTime - beginTime + 1
    val randomDate = beginTime + (Math.random * diff).toLong
    val sdf:SimpleDateFormat = new SimpleDateFormat("dd/MM/yy hh:mm")
    sdf.format(randomDate)
  }

  def randomDouble(min:Double, max:Double) : String = {
    val randomValue = min + (max - min) * Random.nextDouble
    randomValue.toString
  }

  def getRandomTransaction: String = {

    /* Sample
    Transaction_date,Product,Price,Payment_Type,Name,City,Account_Created,Last_Login,Latitude,Longitude,Description
    1/2/09 6:17,Product1,1200,Mastercard,carolina,Tel Aviv,123,1/2/09 6:08,51.5,-1.1166667,Shopping Mall
    */

    var transaction: String = ""

    transaction = randomDate().concat(sep).concat("Product"+Random.nextInt(3)).concat(sep).concat(Random.nextInt(5000).toString).concat(sep)
        .concat(paymentTypes(Random.nextInt(paymentTypes.length))).concat(sep).concat(Random.alphanumeric take 10 mkString).concat(sep)
        .concat(cities(Random.nextInt(cities.length))).concat(sep).concat(Random.nextInt(1000).toString).concat(sep).concat(randomDate()).concat(sep)
        .concat(randomDouble(-360, 360)).concat(sep).concat(randomDouble(-360, 360)).concat(sep).concat(description(Random.nextInt(description.length)))

    transaction

  }

}
