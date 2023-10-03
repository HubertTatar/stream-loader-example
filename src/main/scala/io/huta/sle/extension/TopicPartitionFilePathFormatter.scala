package io.huta.sle.extension

import com.adform.streamloader.model.StreamRange
import com.adform.streamloader.sink.file.FilePathFormatter
import org.apache.kafka.common.TopicPartition

class TopicPartitionFilePathFormatter(fileExtension: Option[String]) extends FilePathFormatter[TopicPartition] {
  override def formatPath(tp: TopicPartition, recordRanges: Seq[StreamRange]): String = {
    val maxOffset = recordRanges.maxBy(s => s.end.offset)
    val minOffset = recordRanges.minBy(s => s.start.offset)

    val fe = fileExtension.map(f => s".$f").getOrElse("")

    s"${tp.topic()}_${tp.partition()}_${minOffset.start.offset}_${maxOffset.end.offset}$fe"
  }
}
