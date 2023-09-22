package io.huta.sle.deduplication

import io.huta.sle.test.MockKafkaContext
import com.adform.streamloader.model.{StreamRecord, Timestamp}
import com.adform.streamloader.util.Logging
import io.huta.sle.config.Configurations
import io.huta.sle.proto.Greet.GreetRequest
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util.Optional
import java.util.concurrent.TimeUnit

class DeduplicatingRecordBatchingSinkTest extends AnyFunSpec with Matchers with Logging {

  it("should write unique") {
    val filesystem = Configurations.hadoopFileSystem()
    val sink = Configurations.deduplicatingSink(filesystem)
    val kafkaContext = new MockKafkaContext()
    sink.initialize(kafkaContext)
    sink.assignPartitions(Set(new TopicPartition("test_topic", 0)))

    records().foreach(r => sink.write(r))

    log.info("give sink some time to stage file on HDFS")
    Thread.sleep(TimeUnit.SECONDS.toMillis(10))
    sink.close()
  }

  def records(): Seq[StreamRecord] = Seq(
    newStreamRecord("test_topic", 0, 1, Timestamp(0L), "key1", newGreet("greet1").toByteArray),
    newStreamRecord("test_topic", 0, 2, Timestamp(2L), "key2", newGreet("greet2").toByteArray),
    newStreamRecord("test_topic", 0, 3, Timestamp(3L), "key3", newGreet("greet3").toByteArray),
    newStreamRecord("test_topic", 0, 4, Timestamp(4L), "key4", newGreet("greet4").toByteArray),
    newStreamRecord("test_topic", 0, 5, Timestamp(5L), "key5", newGreet("greet5").toByteArray),
    newStreamRecord("test_topic", 0, 1, Timestamp(6L), "key1", newGreet("greet1").toByteArray),
    newStreamRecord("test_topic", 0, 2, Timestamp(7L), "key2", newGreet("greet2").toByteArray),
    newStreamRecord("test_topic", 0, 3, Timestamp(8L), "key3", newGreet("greet3").toByteArray),
    newStreamRecord("test_topic", 0, 4, Timestamp(9L), "key5", newGreet("greet4").toByteArray),
    newStreamRecord("test_topic", 0, 5, Timestamp(10L), "key5", newGreet("greet5").toByteArray),
    newStreamRecord("test_topic", 0, 6, Timestamp(50L), "key6", newGreet("greet6").toByteArray),
    newStreamRecord("test_topic", 0, 7, Timestamp(20L), "key7", newGreet("greet7").toByteArray),
    newStreamRecord("test_topic", 0, 7, Timestamp(20L), "key7", newGreet("greet7").toByteArray),
    newStreamRecord("test_topic", 0, 7, Timestamp(20L), "key7", newGreet("greet7").toByteArray),
    newStreamRecord("test_topic", 0, 7, Timestamp(20L), "key7", newGreet("greet7").toByteArray),
    newStreamRecord("test_topic", 0, 7, Timestamp(20L), "key7", newGreet("greet7").toByteArray)
  )

  def newGreet(name: String) = GreetRequest.newBuilder().setName(name).build()

  def newStreamRecord(
      topic: String,
      partition: Int,
      offset: Long,
      timestamp: Timestamp,
      key: String,
      value: Array[Byte]
  ): StreamRecord = {
    val cr = new ConsumerRecord[Array[Byte], Array[Byte]](
      topic,
      partition,
      offset,
      timestamp.millis,
      TimestampType.CREATE_TIME,
      -1,
      -1,
      key.getBytes("UTF-8"),
      value,
      new RecordHeaders,
      Optional.empty[Integer]
    )
    StreamRecord(cr, timestamp)
  }

}
