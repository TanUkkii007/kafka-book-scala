package tanukkii.kafkabook.example.stream.pageview

import java.util

import org.apache.kafka.common.serialization.Deserializer
import spray.json.JsonReader
import spray.json._

class JsonPOJODeserializer[T](implicit reader: JsonReader[T]) extends Deserializer[T] {
  def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  def close(): Unit = {}

  def deserialize(topic: String, data: Array[Byte]): T = data.toString.parseJson.convertTo[T]
}
