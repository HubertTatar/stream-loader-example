package io.huta.sle.dataproducer

import org.apache.kafka.clients.admin.{AdminClient, NewTopic}

import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

trait SetupTopic {
  def setupTopics(props: Properties, topics: List[String]): Unit = {
    def admin = AdminClient.create(props)
    val topicDefinitions = topics.map { topic =>
      new NewTopic(topic, 3, 3.toShort)
    }

    val result = admin.createTopics(topicDefinitions.asJava)
    result.all().get(10, TimeUnit.SECONDS)
    admin.close()
  }
}
