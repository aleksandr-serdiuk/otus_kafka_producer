import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringSerializer,IntegerSerializer}

import java.util.Properties
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties}
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.module.scala._
import com.fasterxml.jackson.databind.json.JsonMapper

object Producer extends App {

  /*
    #list topics
    kafka-topics.sh --bootstrap-server localhost:29092 --list
    #delete topic
    kafka-topics.sh --bootstrap-server localhost:29092 --topic books --delete

    #create topic
    kafka-topics.sh --bootstrap-server localhost:29092 --topic books --create --partitions 3

    #consume topic
    kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic books
    #consume topic from beginning
    kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic books --from-beginning
    #consume topic detailed
    kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic books --formatter kafka.tools.DefaultMessageFormatter --property print.timestamp=true --property print.key=true --property print.value=true --property print.partition=true --from-beginning
  */

  case class books(name: String, author: String, user_rating: Float, reviews: Int, price: Float, year: Int, genre: String)

  def book_line_update (book_line: String) =
  {
    if (book_line.indexOf("\"") == 0) {
      val idx = book_line.substring(1).indexOf("\"")
      book_line.substring(1, idx + 1).replace(",", "#$#") + book_line.substring(idx + 2)
    }
    else
      book_line

  }

  def genBook(cols: Array[String]): books = {
    books(
      name = cols(0).replace("#$#", ","),
      author = cols(1),
      user_rating = cols(2).toFloat,
      reviews = cols(3).toInt,
      price = cols(4).toFloat,
      year = cols(5).toInt,
      genre = cols(6)
    )

  }

  val bufferedSource = io.Source.fromFile("src/main/resources/bestsellers with categories.csv")
  //var listBooks = List[books]()

  val jackson = new ObjectMapper with ScalaObjectMapper
  jackson.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
  jackson.registerModule(DefaultScalaModule)

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:29092")
  val producer = new KafkaProducer(props, new IntegerSerializer, new StringSerializer)
  val topic_name = "books"

  var i = 0
  for (line <- bufferedSource.getLines.drop(1)) {
    i += 1
    val cols = book_line_update(line).split(",").map(_.trim)
    producer.send(new ProducerRecord(topic_name, i, jackson.writeValueAsString(genBook(cols))))

    //println(s"${cols(0).replace("#$#", ",")}|${cols(1)}|${cols(2)}|${cols(3)}|${cols(4)}|${cols(5)}|${cols(6)}")
    //listBooks = listBooks :+ genBook(cols)
  }

  bufferedSource.close
  producer.close()

}
