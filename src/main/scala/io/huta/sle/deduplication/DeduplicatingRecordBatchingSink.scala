package io.huta.sle.deduplication

import com.adform.streamloader.model.StreamInterval
import com.adform.streamloader.sink.batch.storage.RecordBatchStorage
import com.adform.streamloader.sink.batch.{RecordBatch, RecordBatcher, RecordBatchingSinker}
import com.adform.streamloader.sink.{RewindingPartitionGroupSinker, Sink}
import com.adform.streamloader.source.KafkaContext
import com.adform.streamloader.util.Retry
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.common.TopicPartition

import java.time.Duration
import scala.concurrent.duration._
import scala.jdk.DurationConverters._

class DeduplicatingRecordBatchingSink[+B <: RecordBatch](
    recordBatcher: RecordBatcher[B],
    batchStorage: RecordBatchStorage[B],
    batchCommitQueueSize: Int,
    partitionGrouping: TopicPartition => String,
    retryPolicy: Retry.Policy,
    interval: StreamInterval,
    metricRegistry: MeterRegistry,
    keyCacheSize: Int
) extends RewindingPartitionGroupingSink {

  override def initialize(context: KafkaContext): Unit = {
    batchStorage.initialize(context)
    initializeSink(context, interval)
  }

  final override def groupForPartition(topicPartition: TopicPartition): String =
    partitionGrouping(topicPartition)

  /** Creates a new instance of a [[RewindingPartitionGroupSinker]] for a given partition group. These instances are
    * closed and re-created during rebalance events.
    *
    * @param groupName
    *   Name of the partition group.
    * @param partitions
    *   Partitions in the group.
    * @return
    *   A new instance of a sinker for the given group.
    */
  override def sinkerForPartitionGroup(
      groupName: String,
      groupPartitions: Set[TopicPartition]
  ): RewindingPartitionGroupSinker = {
    val baseSinker = new RecordBatchingSinker[B](
      groupName,
      groupPartitions,
      recordBatcher,
      batchStorage,
      batchCommitQueueSize,
      retryPolicy
    )
    new DeduplicatingRewindingPartitionGroupSinker(baseSinker, interval, metricRegistry, keyCacheSize)
  }

}

object DeduplicatingRecordBatchingSink {
  case class Builder[B <: RecordBatch](
      private val _recordBatcher: RecordBatcher[B],
      private val _batchStorage: RecordBatchStorage[B],
      private val _batchCommitQueueSize: Int,
      private val _partitionGrouping: TopicPartition => String,
      private val _retryPolicy: Retry.Policy,
      private val _interval: StreamInterval,
      private val _metricRegistry: MeterRegistry,
      private val _keyCacheSize: Int
  ) extends Sink.Builder {

    /** Sets the record batcher to use.
      */
    def recordBatcher(batcher: RecordBatcher[B]): Builder[B] = copy(_recordBatcher = batcher)

    /** Sets the storage, e.g. HDFS.
      */
    def batchStorage(storage: RecordBatchStorage[B]): Builder[B] = copy(_batchStorage = storage)

    /** Sets the max number of pending batches queued to be committed to storage. Consumption stops when the queue fills
      * up.
      */
    def batchCommitQueueSize(size: Int): Builder[B] = copy(_batchCommitQueueSize = size)

    /** Sets the retry policy for all retriable operations, i.e. recovery, batch commit and new batch creation.
      */
    def retryPolicy(retries: Int, initialDelay: Duration, backoffFactor: Int): Builder[B] =
      copy(_retryPolicy = Retry.Policy(retries, initialDelay.toScala, backoffFactor))

    /** Sets the partition grouping, can be used to route records to different batches.
      */
    def partitionGrouping(grouping: TopicPartition => String): Builder[B] = copy(_partitionGrouping = grouping)

    def interval(interval: StreamInterval): Builder[B] = copy(_interval = interval)

    def metricRegistry(metricRegistry: MeterRegistry): Builder[B] = copy(_metricRegistry = metricRegistry)

    def keyCacheSize(keyCacheSize: Int): Builder[B] = copy(_keyCacheSize = keyCacheSize)

    def build(): DeduplicatingRecordBatchingSink[B] = {
      if (_recordBatcher == null) throw new IllegalStateException("Must specify a RecordBatcher")
      if (_batchStorage == null) throw new IllegalStateException("Must specify a RecordBatchStorage")
      if (_metricRegistry == null) throw new IllegalStateException("Must specify a RecordBatchStorage")

      new DeduplicatingRecordBatchingSink[B](
        _recordBatcher,
        _batchStorage,
        _batchCommitQueueSize,
        _partitionGrouping,
        _retryPolicy,
        _interval,
        _metricRegistry,
        _keyCacheSize
      )
    }
  }

  def builder[B <: RecordBatch](): Builder[B] = Builder[B](
    _recordBatcher = null,
    _batchStorage = null,
    _batchCommitQueueSize = 1,
    _partitionGrouping = _ => "root",
    _retryPolicy = Retry.Policy(retriesLeft = 5, initialDelay = 1.seconds, backoffFactor = 3),
    _interval = StreamInterval.WatermarkRange(Duration.ofMillis(0)),
    _metricRegistry = null,
    _keyCacheSize = 0
  )
}
