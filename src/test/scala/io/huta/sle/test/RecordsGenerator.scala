package io.huta.sle.test

import com.adform.streamloader.model.{StreamRecord, Timestamp}
import io.huta.sle.proto.ComplexTypeOuterClass.ComplexType
import io.huta.sle.proto.HeaderOuterClass.Header
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType

import java.time.{LocalDateTime, ZoneId}
import java.util.{Optional, UUID}

object RecordsGenerator {
  def records(topics: Seq[String], partitions: Int, numOfMsgs: Int): Seq[StreamRecord] =
    for {
      topic <- topics
      partition <- Range(0, partitions)
      messageNumber <- Range(0, numOfMsgs)
    } yield newStreamRecord(
      topic,
      partition,
      messageNumber,
      Timestamp(0L * messageNumber),
      s"key$partition-$messageNumber",
      newType(
        s"some_name${messageNumber}",
        messageNumber,
        0.4 * messageNumber,
        if (messageNumber % 5 == 0) false else true,
        if (messageNumber % 3 == 0) Seq(messageNumber, messageNumber * 2, messageNumber * 3)
        else if (messageNumber % 7 == 0) Seq()
        else Seq(messageNumber, messageNumber * 5)
      ).toByteArray
    )

  def newType(name: String, id: Long, price: Double, isOn: Boolean, somethings: Seq[Int]): ComplexType = {
    val time = LocalDateTime.now().atZone(ZoneId.of("UTC")).toEpochSecond
    val header = Header
      .newBuilder()
      .setUuid(UUID.randomUUID().toString)
      .setRecentProducer("recent_prod")
      .setOriginalProducer("org_prod")
      .setCreationTime(time)
      .build()

    val builder = ComplexType
      .newBuilder()
      .setHeader(header)
      .setName(name)
      .setId(id)
      .setPrice(price)
      .setIsOn(isOn)

    somethings.foreach(s => builder.addSomethings(s))

    builder.build()
  }

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
      key.getBytes("UTF-8").length,
      key.getBytes("UTF-8").length,
      key.getBytes("UTF-8"),
      value,
      new RecordHeaders,
      Optional.empty[Integer]
    )
    StreamRecord(cr, timestamp)
  }
}
