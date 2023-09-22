package io.huta.sle.deduplication

import com.adform.streamloader.model.{StreamInterval, StreamPosition, StreamRecord}
import com.adform.streamloader.sink.{RewindingPartitionGroupSinker, Sink}
import com.adform.streamloader.source.KafkaContext
import com.adform.streamloader.util.Logging
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

trait RewindingPartitionGroupingSink extends Sink with Logging {
  private val partitionGroups = mutable.HashMap.empty[String, (Set[TopicPartition], RewindingPartitionGroupSinker)]
  private val partitionSinkers = mutable.HashMap.empty[TopicPartition, RewindingPartitionGroupSinker]

  protected var kafkaContext: KafkaContext = _
  protected var streamInterval: StreamInterval = _

  def initializeSink(context: KafkaContext, interval: StreamInterval): Unit = {
    kafkaContext = context
    streamInterval = interval
  }

  def groupForPartition(topicPartition: TopicPartition): String

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
  def sinkerForPartitionGroup(groupName: String, partitions: Set[TopicPartition]): RewindingPartitionGroupSinker

  final override def assignPartitions(partitions: Set[TopicPartition]): Map[TopicPartition, Option[StreamPosition]] = {
    log.info(s"Assigned partitions ${partitions.mkString(", ")}, grouping them")

    val groupPositions = partitions.groupBy(groupForPartition).map { case (group, groupPartitions) =>
      // Check if we already have a sinker for this group, if so, close it and collect the previously owned partitions
      val oldGroupPartitions = partitionGroups
        .get(group)
        .map { case (tps, currentGroupSinker) =>
          log.info(s"Closing existing partition sinker for ${tps.mkString(", ")}")
          currentGroupSinker.close()
          partitionGroups.remove(group)
          tps.foreach(partitionSinkers.remove)
          tps
        }
        .getOrElse(Set.empty)

      log.info(
        s"Creating and initializing a new sinker for partition group '$group' containing " +
          s"newly assigned partitions ${groupPartitions
              .mkString(", ")} and previously owned partitions ${oldGroupPartitions.mkString(", ")}"
      )

      val newGroupPartitions = groupPartitions ++ oldGroupPartitions
      val sinker = sinkerForPartitionGroup(group, newGroupPartitions)
      val positions = sinker.initialize(kafkaContext)

      partitionGroups.put(group, newGroupPartitions -> sinker)
      newGroupPartitions.foreach(tp => partitionSinkers.put(tp, sinker))
      positions
    }

    groupPositions.flatMap(_.toList).toMap
  }

  final override def revokePartitions(partitions: Set[TopicPartition]): Map[TopicPartition, Option[StreamPosition]] = {
    if (partitions.nonEmpty) {
      log.info(s"Revoked partitions ${partitions.mkString(", ")}")

      val groupPositions = partitions.groupBy(groupForPartition).map { case (group, _) =>
        val remainingGroupPartitions = partitionGroups
          .get(group)
          .map { case (currentGroupPartitions, currentGroupSinker) =>
            log.info(s"Closing existing group '$group' partition sinker for ${currentGroupPartitions.mkString(", ")}")
            currentGroupSinker.close()
            partitionGroups.remove(group)
            currentGroupPartitions.foreach(partitionSinkers.remove)
            currentGroupPartitions -- partitions
          }
          .getOrElse(Set.empty)

        // The group still contains partitions, re-create the sinker with the remaining set
        if (remainingGroupPartitions.nonEmpty) {
          log.info(
            s"Creating and initializing a new sinker for partition group '$group' containing " +
              s"remaining partitions ${remainingGroupPartitions.mkString(", ")}"
          )

          val sinker = sinkerForPartitionGroup(group, remainingGroupPartitions)
          val positions = sinker.initialize(kafkaContext)

          partitionGroups.put(group, remainingGroupPartitions -> sinker)
          remainingGroupPartitions.foreach(tp => partitionSinkers.put(tp, sinker))

          positions
        } else {
          Map.empty
        }
      }

      groupPositions.flatMap(_.toList).toMap
    } else {
      Map.empty
    }
  }

  /** Forwards a consumer record to the correct partition sinker.
    *
    * @param record
    *   Stream record to write.
    */
  override def write(record: StreamRecord): Unit = {
    partitionSinkers(record.topicPartition).write(record)
  }

  /** Forwards the heartbeat to all sinkers.
    */
  override def heartbeat(): Unit = {
    partitionSinkers.foreach { case (_, sinker) => sinker.heartbeat() }
  }

  /** Closes all active sinkers.
    */
  override def close(): Unit = {
    partitionGroups.foreach { case (group, (groupPartitions, sinker)) =>
      log.info(s"Closing sinker for group '$group' containing partitions ${groupPartitions.mkString(",")}")
      sinker.close()
    }
  }
}
