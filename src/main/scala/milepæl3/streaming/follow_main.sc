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


System.setProperty("java.security.auth.login.config", "D:\\projects_git\\Semester5\\big_data\\jaas.conf")
Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)

val username = "aneqi8m2"
val password = "tiYqB_68T6l8OZU30p22LqTrXAsfEmCJ"
val topic = "aneqi8m2-news"

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
print("Starting stream\n")
streamingContext.start()
print("Printing messages: \n")
streamingContext.awaitTermination()