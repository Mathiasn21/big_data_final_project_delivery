package milepæl3

object KafkaConfig {
  val username = "aneqi8m2"
  val password = "tiYqB_68T6l8OZU30p22LqTrXAsfEmCJ"
  val topic = "aneqi8m2-news"
  val jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required serviceName=\"kafka\" username=\"%s\" password=\"%s\";"
  val jaasCfg: String = String.format(jaasTemplate, username, password)
}
