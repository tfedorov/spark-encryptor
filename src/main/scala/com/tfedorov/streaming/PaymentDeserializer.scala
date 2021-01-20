package com.tfedorov.streaming

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Deserializer

protected[streaming] class PaymentDeserializer extends Deserializer[Payment] {

  override def deserialize(topic: String, data: Array[Byte]): Payment = {
    val decoded = PaymentDeserializer.deserializer.deserialize(topic, data)
    val id = decoded.get("id").asInstanceOf[org.apache.avro.util.Utf8].toString
    val amount = decoded.get("amount").asInstanceOf[Double]
    Payment(id, amount)
  }
}

protected[streaming] object PaymentDeserializer {

  var deserializer: Deserializer[GenericRecord] = _

  def init(schemaRegUrl: String) = {
    val client = new CachedSchemaRegistryClient(schemaRegUrl, 100)
    deserializer = new KafkaAvroDeserializer(client).asInstanceOf[Deserializer[GenericRecord]]
  }
}