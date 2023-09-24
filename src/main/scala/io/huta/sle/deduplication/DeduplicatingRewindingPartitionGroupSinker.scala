package io.huta.sle.deduplication

import com.adform.streamloader.model.{StreamInterval, StreamRecord}
import com.adform.streamloader.sink.{PartitionGroupSinker, RewindingPartitionGroupSinker}
import com.adform.streamloader.util.Logging
import io.micrometer.core.instrument.Counter

class DeduplicatingRewindingPartitionGroupSinker(
    baseSinker: PartitionGroupSinker,
    interval: StreamInterval,
    counter: Counter,
    keyCacheSize: Int
) extends RewindingPartitionGroupSinker(baseSinker, interval)
    with Logging {

  private val cache: FifoHashSet[String] = FifoHashSet.apply(keyCacheSize)

  override def write(record: StreamRecord): Unit = {
    val key = record.consumerRecord.key()
    val ketStr = new String(key)
    if (!cache.contains(ketStr)) {
      super.write(record)
    } else {
      counter.increment()
    }
    cache.add(ketStr)
  }

  override protected def touchRewoundRecord(record: StreamRecord): Unit =
    cache.add(new String(record.consumerRecord.key()))
}
