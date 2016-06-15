package tanukkii.kafkabook.example.stream.pageview

import java.nio.charset.Charset
import java.util
import org.apache.kafka.common.serialization.Serializer
import spray.json.JsonWriter
import spray.json._

class JsonPOJOSerializer[T](implicit writer: JsonWriter[T]) extends Serializer[T] {
  val utf8 = Charset.forName("UTF-8")

  def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {

  }

  def serialize(topic: String, data: T): Array[Byte] = data.toJson.prettyPrint.getBytes(utf8)

  def close(): Unit = {}
}
