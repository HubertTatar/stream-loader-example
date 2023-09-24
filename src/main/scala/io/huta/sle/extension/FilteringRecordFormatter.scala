package io.huta.sle.extension

import com.adform.streamloader.model.StreamRecord
import com.adform.streamloader.sink.batch.RecordFormatter
import com.google.protobuf.Message
import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException

import java.lang.reflect.InvocationTargetException
import scala.reflect.ClassTag

/*
  Extend to implement filtering
 */
class FilteringRecordFormatter[R <: Message: ClassTag] extends RecordFormatter[AnnotatedProtoRecord[R]] {

  private val recordClass = implicitly[ClassTag[R]].runtimeClass.asInstanceOf[Class[_ <: Message]]
  private val recordParseMethod = recordClass.getMethod("parseFrom", classOf[Array[Byte]])

  def passRecords(record: R): Boolean = true

  override def format(record: StreamRecord): Seq[AnnotatedProtoRecord[R]] = {
    try {
      val parsed = recordParseMethod.invoke(null, record.consumerRecord.value()).asInstanceOf[R]
      val metadata = KafkaMetadata(
        record.consumerRecord.topic(),
        record.consumerRecord.partition(),
        record.consumerRecord.offset(),
        record.consumerRecord.timestamp(),
        record.watermark.millis
      )
      Seq(AnnotatedProtoRecord(parsed, metadata))
        .filter(r => passRecords(r.record))
    } catch {
      case e: InvalidProtocolBufferException =>
        logInvalidMessage(record, e)
        Seq.empty
      case e: InvocationTargetException if e.getCause.isInstanceOf[InvalidProtocolBufferException] =>
        logInvalidMessage(record, e)
        Seq.empty
    }
  }

  private def logInvalidMessage(record: StreamRecord, e: Throwable): Unit = {
    println(s"Invalid proto message in ${record.consumerRecord.topic()}-${record.consumerRecord
        .partition()} at offset ${record.consumerRecord.offset()}, skipping")
  }

}
