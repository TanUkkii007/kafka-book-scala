package tanukkii.kafkabook.example.stream
package pageview

// source: https://github.com/apache/kafka/blob/0.10.0/streams/examples/src/main/java/org/apache/kafka/streams/examples/pageview/PageViewTypedDemo.java

import java.lang

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.apache.kafka.streams.kstream.{KStream, Windowed, TimeWindows, KStreamBuilder}
import spray.json.DefaultJsonProtocol
import scala.collection.JavaConverters._

object PageViewTypedDemo extends App
with DefaultJsonProtocol
with ValueJoinerConversion
with KeyValueMapperConversion {

  case class PageView(user: String, page: String, timestamp: Long)

  case class UserProfile(region: String, timestamp: Long)

  case class PageViewByRegion(user: String, page: String, region: String)

  case class WindowedPageViewByRegion(windowStart: Long, region: String)

  case class RegionCount(count: Long, region: String)

  implicit val PageViewJsonFormat = jsonFormat3(PageView)
  implicit val UserProfileFormat = jsonFormat2(UserProfile)
  implicit val PageViewRegionFormat = jsonFormat3(PageViewByRegion)
  implicit val WindowedPageViewByRegionFormat = jsonFormat2(WindowedPageViewByRegion)
  implicit val RegionCountFormat = jsonFormat2(RegionCount)


  val props: Map[String, AnyRef] = Map(
    StreamsConfig.APPLICATION_ID_CONFIG -> "streams-pageview-typed",
    StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    StreamsConfig.ZOOKEEPER_CONNECT_CONFIG -> "localhost:2181",
    StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG -> classOf[JsonTimestampExtractor],
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
  )

  val builder = new KStreamBuilder()

  val serdeProps = new java.util.HashMap[String, Any]()

  val pageViewSerializer = new JsonPOJOSerializer[PageView]()
  serdeProps.put("JsonPOJOClass", classOf[PageView])
  pageViewSerializer.configure(serdeProps, false)

  val pageViewDeserializer = new JsonPOJODeserializer[PageView]()

  val pageViewSerde = Serdes.serdeFrom(pageViewSerializer, pageViewDeserializer)

  val userProfileSerializer = new JsonPOJOSerializer[UserProfile]
  val userProfileDeserializer = new JsonPOJODeserializer[UserProfile]
  val userProfileSerde = Serdes.serdeFrom(userProfileSerializer, userProfileDeserializer)

  val wPageViewByRegionSerializer = new JsonPOJOSerializer[WindowedPageViewByRegion]()
  val wPageViewByRegionDeserializer = new JsonPOJODeserializer[WindowedPageViewByRegion]()
  val wPageViewByRegionSerde = Serdes.serdeFrom(wPageViewByRegionSerializer, wPageViewByRegionDeserializer)

  val regionCountSerializer = new JsonPOJOSerializer[RegionCount]()
  val regionCountDeserializer = new JsonPOJODeserializer[RegionCount]()
  val regionCountSerde = Serdes.serdeFrom(regionCountSerializer, regionCountDeserializer)

  val views = builder.stream(Serdes.String(), pageViewSerde, "streams-pageview-input")

  val users = builder.table(Serdes.String(), userProfileSerde, "streams-userprofile-input")

  val regionCount: KStream[WindowedPageViewByRegion, RegionCount] = views.leftJoin(users, (view: PageView, profile: UserProfile) => {
    PageViewByRegion(user = view.user, page = view.page, region = profile.region)
  }).map((user: String, viewRegion: PageViewByRegion) => {
    new KeyValue(viewRegion.region, viewRegion)
  }).countByKey(TimeWindows.of("GeoPageViewsWindow", 7 * 24 * 60 * 60 * 1000L).advanceBy(1000), Serdes.String())
  .toStream()
  .map((key: Windowed[String], value: lang.Long) => {
    val wViewByRegion = WindowedPageViewByRegion(windowStart = key.window().start(), region = key.key())
    val rCount = RegionCount(region = key.key(), count = value)
    println(wViewByRegion, rCount)
    new KeyValue(wViewByRegion, rCount)
  })

  regionCount.to(wPageViewByRegionSerde, regionCountSerde, "streams-pageviewstats-typed-output")

  val streams = new KafkaStreams(builder, new StreamsConfig(props.asJava))
  streams.start()

  Thread.sleep(5000)

  streams.close()
}
