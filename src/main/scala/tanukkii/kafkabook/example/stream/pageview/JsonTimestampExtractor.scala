package tanukkii.kafkabook.example.stream.pageview

import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor

class JsonTimestampExtractor extends TimestampExtractor {
  def extract(record: ConsumerRecord[AnyRef, AnyRef]): Long = {
    record.value() match {
      case pv: PageViewTypedDemo.PageView => pv.timestamp
      case up: PageViewTypedDemo.UserProfile => up.timestamp
      case jn: JsonNode => jn.get("timestamp").longValue()
      case rv => throw new IllegalArgumentException("JsonTimestampExtractor cannot recognize the record value " + rv)
    }
  }
}
