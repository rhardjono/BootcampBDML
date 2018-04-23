package com.rhardjono.keepcoding.domain.serde

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util

import com.rhardjono.keepcoding.domain.Customer
import org.apache.kafka.common.serialization.Serializer


class CustomerSerializer extends Serializer[Customer] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {

  }

  override def serialize(topic: String, data: Customer): Array[Byte] = {
    try {
      val byteOut = new ByteArrayOutputStream()
      val objOut = new ObjectOutputStream(byteOut)
      objOut.writeObject(data)
      objOut.close()
      byteOut.close()
      byteOut.toByteArray
    }
    catch {
      case ex: Exception => throw new Exception(ex.getMessage)
    }
  }

  override def close(): Unit = {

  }

}