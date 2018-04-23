package com.rhardjono.keepcoding.batchlayer


import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.rhardjono.keepcoding.domain.{Customer, Geolocation, Metrics, Transaction}
import com.rhardjono.keepcoding.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.apache.spark.sql.functions.udf


class MetricsSQL extends Serializable {

  private val metricConf = new AppConfig()

  def run(args: Array[String]): Unit = {

    val session = SparkSession.builder
      .master("local[*]")
      .appName("Final Project - Batch Layer")
      .getOrCreate()

    session.sparkContext.setLogLevel("WARN")

    import session.implicits._

    val pathSave = s"file:///${args(1)}"
    val rawData = session.read.csv(s"file:///${args(0)}")
    val header = rawData.first()
    val rawDataWithoutHeader = rawData.filter(!_.equals(header)).map(_.toString().split(","))

    val transactionAcc: LongAccumulator = session.sparkContext.longAccumulator("idTransaction")
    val sdf: SimpleDateFormat = new SimpleDateFormat("dd/MM/yy HH:mm")
    val metrics = new Metrics(session)

    /********************
      *  TRANSACTIONS  **
      *******************/
    val transactionDF = rawDataWithoutHeader.map(col => {
      transactionAcc.add(1)
      Transaction(transactionAcc.value,
        new Timestamp(sdf.parse(col(0).replace("[", "")).getTime()),
        col(4).trim.toUpperCase,
        col(2).toDouble,
        col(10).replace("]", "").trim,
        metrics.getCategory(col(10).replace("]", "").trim),
        col(3).trim,
        Geolocation(col(8).toDouble, col(9).toDouble, col(5).trim, metrics.getCountryByCoordinates(col(8).toDouble, col(9).toDouble, metricConf.envOrElseConfig("google.apiKey"))))
    })
    //transactionDF.show(false)

    /*****************
      *  CUSTOMERS  **
      ****************/
    val customerDF = rawDataWithoutHeader.map(col => {
      Customer(col(4).trim.toUpperCase, col(6), col(1))
    })
    //customerDF.show(false)

    /* Customer's accounts */
    val accountsByCustomer = customerDF.groupBy("name").count().orderBy($"count".desc).withColumnRenamed("count", "Customer Accounts")
    //accountsByCustomer.show(false)

    /* Customers with no duplicated names */
    //val customers1 = customerDF.dropDuplicates("name").select("name")
    //customers1.show(false)

    /* Customers with no duplicated accounts */
    //val customers2 = customerDF.dropDuplicates("account")
    //customers2.show(false)

    /* Customers with no duplicated names & accounts */
    //val customers3 = customerDF.dropDuplicates(Seq("name", "account"))
    //customers3.show(false)

    /* Example of UDF usage */
    //transactionDF.drop("category").withColumn("category", metrics.getCategoryUDF(transactionDF("description"))).show

    transactionDF.createOrReplaceGlobalTempView("TRANSACTIONS")
    customerDF.createOrReplaceGlobalTempView("CUSTOMERS")

    /* METRICA#1 */
    val tareaSQL1 =  metrics.metricaSQL1()
    println("METRICA#1: Transacciones por ciudad")
    tareaSQL1.show(false)

    /* METRICA#2 */
    val tareaSQL2 =   metrics.metricaSQL2()
    println("METRICA#2: Clientes que han realizado pagos superiores a 3000")
    tareaSQL2.show(false)

    /* METRICA#3 */
    val tareaSQL3 =   metrics.metricaSQL3()
    println("METRICA#3: Transacciones realizadas en la ciudad de New York")
    tareaSQL3.show(false)

    /* METRICA#4 */
    val tareaSQL4 =  metrics.metricaSQL4()
    println("METRICA#4: Transacciones cuya categoria sea Ocio")
    tareaSQL4.show(false)

    /* METRICA#5 */
    val tareaSQL5 =  metrics.metricaSQL5()
    println("METRICA#5: Transacciones de cada cliente en los ultimos 30 dias")
    tareaSQL5.show(false)

    /**********************************
     * Write & Read File Formats Test *
    ***********************************/

    // PARQUET
    /*
    metrics.writeParquet(tareaSQL1, pathSave+"/parquet/metrica1")
    val parquetDF = metrics.readParquet( pathSave+"/parquet//metrica5")
    parquetDF.show()
    */

    // AVRO
    /*
    metrics.writeAvro(tareaSQL1, pathSave+"/avro/metrica1")
    val avroDF = metrics.readAvro( pathSave+"/avro//metrica1")
    avroDF.show()
    */

    // ORC
    /*
    metrics.writeORC(tareaSQL1, pathSave+"/orc/metrica1")
    val orcDF = metrics.readORC( pathSave+"/orc//metrica1")
    orcDF.show()
    */

    // Persistent Tables (Hive)
    /*
    metrics.writeTable(tareaSQL1, "tblMetrica")
    val resultDF = metrics.readTable("tblMetrica")
    resultDF.show()
    */


  }

}

