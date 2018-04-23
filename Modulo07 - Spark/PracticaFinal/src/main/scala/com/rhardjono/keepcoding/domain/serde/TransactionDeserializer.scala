package com.rhardjono.keepcoding.domain.serde

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util

import com.rhardjono.keepcoding.domain.Transaction
import org.apache.kafka.common.serialization.Deserializer

class TransactionDeserializer extends Deserializer[Transaction] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {

  }

  override def deserialize(topic: String, bytes: Array[Byte]) = {
    val byteIn = new ByteArrayInputStream(bytes)
    val objIn = new ObjectInputStream(byteIn)
    val obj = objIn.readObject().asInstanceOf[Transaction]
    byteIn.close()
    objIn.close()
    obj
  }

  override def close(): Unit = {

  }

}



