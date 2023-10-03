package io.huta.sle.extension

import io.huta.sle.test.RecordsGenerator._
import io.huta.sle.test.MockKafkaContext
import com.adform.streamloader.util.Logging
import io.huta.sle.config.SimpleConfiguration
import org.apache.kafka.common.TopicPartition
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.TimeUnit

class TopicPartitionFilePathFormatterTest extends AnyFunSpec with Matchers with Logging {

  it("should partition one topic and three partitions") {
    val filesystem = SimpleConfiguration.hadoopFileSystem()
    val sink = SimpleConfiguration.buildSinkByTopicPartition(filesystem)
    val kafkaContext = new MockKafkaContext()
    val topics = Seq("test_topic_1")
    val partitions = 3

    val data = records(topics, partitions, 10)

    sink.initialize(kafkaContext)

    val assigment = for {
      topic <- topics
      partition <- Range(0, partitions)
    } yield new TopicPartition(topic, partition)

    sink.assignPartitions(assigment.toSet)

    data.foreach(r => sink.write(r))

    log.info("give sink some time to stage file on HDFS")
    Thread.sleep(TimeUnit.SECONDS.toMillis(10))
    sink.close()
  }

  it("should partition three topic and three partitions") {
    val filesystem = SimpleConfiguration.hadoopFileSystem()
    val sink = SimpleConfiguration.buildSinkByTopicPartition(filesystem)
    val kafkaContext = new MockKafkaContext()
    val topics = Seq("test_topic_1", "test_topic_2", "test_topic_3")
    val partitions = 3
    val data = records(topics, partitions, 10)

    sink.initialize(kafkaContext)
    val assigment = for {
      topic <- topics
      partition <- Range(0, partitions)
    } yield new TopicPartition(topic, partition)

    sink.assignPartitions(assigment.toSet)

    data.foreach(r => sink.write(r))

    log.info("give sink some time to stage file on HDFS")
    Thread.sleep(TimeUnit.SECONDS.toMillis(10))
    sink.close()
  }
}
