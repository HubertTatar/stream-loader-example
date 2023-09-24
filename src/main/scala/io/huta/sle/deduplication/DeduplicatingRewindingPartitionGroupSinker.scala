package io.huta.sle.deduplication

import com.adform.streamloader.model.{StreamInterval, StreamRecord}
import com.adform.streamloader.sink.{PartitionGroupSinker, RewindingPartitionGroupSinker}
import com.adform.streamloader.util.Logging
import io.micrometer.core.instrument.MeterRegistry

//todo pass metrics, remove logging
class DeduplicatingRewindingPartitionGroupSinker(
    baseSinker: PartitionGroupSinker,
    interval: StreamInterval,
    metricRegistry: MeterRegistry,
    keyCacheSize: Int
) extends RewindingPartitionGroupSinker(baseSinker, interval)
    with Logging {

  private val cache: FifoHashSet[String] = FifoHashSet.apply(keyCacheSize)

  override def write(record: StreamRecord): Unit = {
    val key = record.consumerRecord.key()
    val ketStr = new String(key)
    if (!cache.contains(ketStr)) {
      log.info(s"Not found in cache, writing $ketStr")
      super.write(record)
    } else {
      log.info(s"Duplicate found $ketStr")
    }
    cache.add(ketStr)
  }

  override protected def touchRewoundRecord(record: StreamRecord): Unit =
    cache.add(new String(record.consumerRecord.key()))
}
