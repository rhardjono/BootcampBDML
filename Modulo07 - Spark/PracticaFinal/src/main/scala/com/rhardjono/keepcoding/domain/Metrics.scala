package com.rhardjono.keepcoding.domain

import com.rhardjono.keepcoding.geocoder.Geocoder
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import com.databricks.spark.avro._

class Metrics(sesion: SparkSession) extends Serializable {

  def getCategory(desc: String): String = desc match {
    case "Cinema" | "Sports" | "Restaurant" => "Ocio"
    case "car insurace" | "home insurance" | "life insurance" | "insurance" => "Seguro"
    case "leasing" => "Alquiler"
    case _ => "N/A"
  }

  //TODO: no aplica uso de UDF
  val getCategoryUDF = udf[String, String](getCategory)

  def getCountryByCoordinates(latitude: Double, longitude: Double, apiKey: String): String = {

    var mockMode = false
    var country = "N/A"

    if (!mockMode) {
      // The factory object can be used to lazily create Geocoders
      val geo = Geocoder.create(apiKey)
      val response = geo.lookup(latitude, longitude)

      //ex: AddressComponent(United Kingdom,GB,List(country, political))
      var countryAddressComponent = response.head.addressComponents.filter(x => x.types(0) == "country")
      country = countryAddressComponent(0).longName
    }
    country
  }

  def metricaSQL1(): DataFrame = {
    sesion.sql("SELECT geolocation.city, COUNT(name) AS total_transactions FROM global_temp.TRANSACTIONS GROUP BY geolocation.city ")
  }

  def metricaSQL2(): DataFrame = {
    sesion.sql("SELECT T.*, C.account, C.product FROM global_temp.TRANSACTIONS T LEFT JOIN global_temp.CUSTOMERS C ON T.name = C.name WHERE T.price > 3000")
  }

  def metricaSQL3(): DataFrame = {
    //sesion.sql("SELECT name FROM global_temp.TRANSACTIONS WHERE geolocation.city = 'New York' GROUP BY name")
    sesion.sql("SELECT geolocation.city, COUNT(*) as total FROM global_temp.TRANSACTIONS WHERE geolocation.city = 'New York' GROUP BY geolocation.city  ")
  }

  def metricaSQL4(): DataFrame = {
    sesion.sql("SELECT * FROM global_temp.TRANSACTIONS WHERE category = 'Ocio'")
  }

  def metricaSQL5(): DataFrame = {
    //sesion.sql("SELECT * FROM global_temp.TRANSACTIONS T LEFT JOIN global_temp.CUSTOMERS C ON T.name = C.name WHERE transactionDate <= current_timestamp() AND transactionDate > date_sub(current_timestamp(), 30) ")
    sesion.sql("SELECT * FROM global_temp.TRANSACTIONS WHERE transactionDate <= current_timestamp() AND transactionDate > date_sub(current_timestamp(), 30) ")
  }

  def metricaSQL6(): DataFrame = {
    sesion.sql("SELECT name, avg(price) AS gasto_medio FROM global_temp.TRANSACTIONS WHERE geolocation.country = 'United Kingdom' AND category = 'Ocio' GROUP BY name")
  }

  def metricaSQL7(): DataFrame = {
    sesion.sql("SELECT name, SUM(price) AS total_gasto FROM global_temp.TRANSACTIONS WHERE category LIKE 'Seguro' GROUP BY name HAVING SUM(PRICE) >= 5000")
  }

  /** *****************************
    * FILE FORMATS READ & WRITE  *
    * ******************************/

  def writeAvro(df: DataFrame, path: String): Unit = {
    df.write.mode(SaveMode.Overwrite).avro(path)
    //df.write.mode(SaveMode.Overwrite).format("com.databricks.spark.avro").save(path)
  }

  def readAvro(path: String): DataFrame = {
    sesion.read.avro(path)
  }

  def writeParquet(df: DataFrame, path: String): Unit = {
    df.write.mode(SaveMode.Overwrite).parquet(path)
    //df.write.mode(SaveMode.Overwrite).format("parquet").save(path)
  }

  def readParquet(path: String): DataFrame = {
    sesion.read.parquet(path)
  }

  def writeORC(df: DataFrame, path: String): Unit = {
    df.write.mode(SaveMode.Overwrite).orc(path)
    //df.write.mode(SaveMode.Overwrite).format("parquet").save(path)
  }

  def readORC(path: String): DataFrame = {
    sesion.read.orc(path)
  }

  def writeTable(df: DataFrame, table: String): Unit = {
    df.write.mode(SaveMode.Overwrite).saveAsTable(table)
    //df.write.mode(SaveMode.Overwrite).format("parquet").save(path)
  }

  def readTable(table: String): DataFrame = {
    sesion.sql("select * from " + table)
  }


}
