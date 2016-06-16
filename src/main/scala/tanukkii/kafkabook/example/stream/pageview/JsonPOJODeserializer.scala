package tanukkii.kafkabook.example.stream.pageview

import java.nio.charset.Charset
import java.util

import org.apache.kafka.common.serialization.Deserializer
import spray.json.JsonReader
import spray.json._

class JsonPOJODeserializer[T](implicit reader: JsonReader[T]) extends Deserializer[T] {
  val utf8 = Charset.forName("UTF-8")

  def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  def close(): Unit = {}

  def deserialize(topic: String, data: Array[Byte]): T = new String(data, 0, data.length, utf8).parseJson.convertTo[T]
}
