//Credentials(Uname, password, topic)

import org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider.JaasConfiguration
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent


Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)

val username = "aneqi8m2"
val password = "tiYqB_68T6l8OZU30p22LqTrXAsfEmCJ"
val topic = "aneqi8m2-news"
val jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required serviceName=\"kafka\" username=\"%s\" password=\"%s\";"
val jaasCfg = String.format(jaasTemplate, username, password)

/*
val spark = SparkSession.builder
  .master("local[*]")
  .appName("Testing App")
  .getOrCreate()*/

val sparkConf = new SparkConf().setMaster("local[*]").setAppName("DirectKafkaWordCount")
val streamingContext = new StreamingContext(sparkConf, Seconds(2))
val kafkaParams = Map[String, Object](
  "bootstrap.servers" -> "rocket-01.srvs.cloudkafka.com:9094,rocket-02.srvs.cloudkafka.com:9094,rocket-03.srvs.cloudkafka.com:9094",
  "key.deserializer" -> classOf[StringDeserializer],
  "value.deserializer" -> classOf[StringDeserializer],
  "group.id" -> "use_a_separate_group_id_for_each_stream",
  "auto.offset.reset" -> "latest",
  "security.protocol" -> "SASL_SSL",
  "sasl.mechanisms" -> "SCRAM-SHA-256",
  "sasl.jaas.config" -> jaasCfg,
  "sasl.username" -> username,
  "sasl.password" -> password,
  "enable.auto.commit" -> (false: java.lang.Boolean)
)


val topics = Array(topic)
val stream = KafkaUtils.createDirectStream[String, String](
  streamingContext,
  PreferConsistent,
  Subscribe[String, String](topics, kafkaParams)
)

stream.print()
streamingContext.start()
print("Starting stream\n")
streamingContext.awaitTermination()