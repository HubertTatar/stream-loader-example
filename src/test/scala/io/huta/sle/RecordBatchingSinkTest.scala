package io.huta.sle

import com.adform.streamloader.util.Logging
import io.huta.sle.config.SimpleConfiguration
import io.huta.sle.test.MockKafkaContext
import org.apache.kafka.common.TopicPartition
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import io.huta.sle.test.RecordsGenerator._

import java.util.concurrent.TimeUnit

class RecordBatchingSinkTest extends AnyFunSpec with Matchers with Logging {
  it("should write unique") {
    val filesystem = SimpleConfiguration.hadoopFileSystem()
    val sink = SimpleConfiguration.deduplicatingSink(filesystem)
    val kafkaContext = new MockKafkaContext()
    val topic = Seq("test_topic")
    val partitions = 3

    sink.initialize(kafkaContext)
    val assignment = for {
      t <- topic
      p <- Range(0, partitions)
    } yield new TopicPartition(t, p)
    sink.assignPartitions(assignment.toSet)

    records(topic, partitions, 10).foreach(r => sink.write(r))

    log.info("give sink some time to stage file on HDFS")
    Thread.sleep(TimeUnit.SECONDS.toMillis(10))
    sink.close()
  }
}
