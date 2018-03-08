/*
 * Copyright 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbmhtechnology.eventuate.log.rocksdb

import java.io.Closeable
import java.nio.file.{ Files, Paths }
import java.util.concurrent.TimeUnit
import java.util.{ ArrayList => JList }

import akka.actor.{ Actor, ActorRef, Props, Status }
import akka.pattern._
import akka.serialization.SerializationExtension
import com.rbmhtechnology.eventuate.DurableEvent
import com.rbmhtechnology.eventuate.log._
import com.typesafe.config.Config
import org.rocksdb._

import scala.collection.immutable.{ Seq, VectorBuilder }
import scala.concurrent.duration._
import scala.concurrent.{ Future, Promise }
import scala.util.{ Failure, Success, Try }

/**
 * @author siuming
 */
class RocksdbEventLogSettings(config: Config) extends EventLogSettings {

  val readTimeout: FiniteDuration =
    config.getDuration("eventuate.log.read-timeout", TimeUnit.MILLISECONDS).millis

  val prefix: String =
    config.getString("eventuate.log.rocksdb.prefix")

  val rootDir: String =
    config.getString("eventuate.log.rocksdb.dir")

  val fsync: Boolean =
    config.getBoolean("eventuate.log.rocksdb.fsync")

  val stateSnapshotLimit: Int =
    config.getInt("eventuate.log.rocksdb.state-snapshot-limit")

  val deletionBatchSize: Int =
    config.getInt("eventuate.log.rocksdb.deletion-batch-size")

  val initRetryDelay: FiniteDuration =
    Duration.Zero

  val initRetryMax: Int =
    0

  val deletionRetryDelay: FiniteDuration =
    config.getDuration("eventuate.log.rocksdb.deletion-retry-delay", TimeUnit.MILLISECONDS).millis

  val partitionSize: Long =
    Long.MaxValue
}
case class RocksdbEventLogState(eventLogClock: EventLogClock, deletionMetadata: DeletionMetadata) extends EventLogState
object RocksdbEventLog {

  private[rocksdb] val clockKeyBytes: Array[Byte] =
    EventKeys.eventKeyBytes(0, 0L)

  private[rocksdb] def completed[A](body: => A): Future[A] =
    Future.fromTry(Try(body))

  /**
   * Creates a [[RocksdbEventLog]] configuration object.
   *
   * @param logId unique log id.
   * @param batching `true` if write-batching shall be enabled (recommended).
   */
  def props(logId: String, batching: Boolean = true): Props = {
    val logProps = Props(new RocksdbEventLog(logId)).withDispatcher("eventuate.log.dispatchers.write-dispatcher")
    if (batching) Props(new BatchingLayer(logProps)) else logProps
  }
}
class RocksdbEventLog(id: String) extends EventLog[RocksdbEventLogState](id) with RocksdbBatchLayer {

  import EventKeys._
  import EventValues._
  import RocksdbEventLog._

  override val settings = new RocksdbEventLogSettings(context.system.settings.config)
  private implicit val serialization = SerializationExtension(context.system)

  // default column family must specified.
  private val columnFamilies = new JList[ColumnFamilyDescriptor]() {
    add(0, new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions().useFixedLengthPrefixExtractor(4)))
    add(1, new ColumnFamilyDescriptor(stringBytes("aggregates"), new ColumnFamilyOptions().setMergeOperatorName("uint64add")))
    add(2, new ColumnFamilyDescriptor(stringBytes("progresses")))
    add(3, new ColumnFamilyDescriptor(stringBytes("metadata")))
  }
  private val columnHandles = new JList[ColumnFamilyHandle]()

  private val rocksdbDir = Paths.get(settings.rootDir, s"${settings.prefix}_$id"); Files.createDirectories(rocksdbDir)
  private val rocksdbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
  protected val rocksdb = RocksDB.open(rocksdbOptions, rocksdbDir.toAbsolutePath.toString, columnFamilies, columnHandles)
  protected val writeOptions = new WriteOptions().setSync(settings.fsync)

  private val numericStore = new NumericIdStore(rocksdb, writeOptions, columnHandles.get(1))
  private val progressStore = new ProgressStore(rocksdb, writeOptions, columnHandles.get(2))
  private val deletionStore = new DeletionStore(rocksdb, writeOptions, columnHandles.get(3))

  private var updateCount: Long = 0L

  override def preStart(): Unit = {
    rocksdb.put(columnHandles.get(0), eventKeyEndBytes, Array.empty[Byte])
    super.preStart()
  }

  /**
   * Asynchronously recovers event log state from the storage backend.
   */
  override def recoverState = completed {
    val clockSnapshot = readEventLogClockSnapshotSync
    val clockRecovered = withIterator(clockSnapshot.sequenceNr + 1L, EventKeys.DefaultClassifier) { iter =>
      iter.foldLeft(clockSnapshot)(_ update _)
    }
    RocksdbEventLogState(clockRecovered, deletionStore.readDeletion())
  }

  /**
   * Asynchronously reads all stored local replication progresses.
   *
   * @see [[com.rbmhtechnology.eventuate.ReplicationProtocol.GetReplicationProgresses]]
   */
  override def readReplicationProgresses =
    completed(progressStore.readProgresses())

  /**
   * Asynchronously reads the replication progress for given source `logId`.
   *
   * @see [[com.rbmhtechnology.eventuate.ReplicationProtocol.GetReplicationProgress]]
   */
  override def readReplicationProgress(logId: String) =
    completed(progressStore.readProgress(logId))

  /**
   * Asynchronously batch-reads events from the raw event log. At most `max` events must be returned that are
   * within the sequence number bounds `fromSequenceNr` and `toSequenceNr`.
   *
   * @param fromSequenceNr sequence number to start reading (inclusive).
   * @param toSequenceNr   sequence number to stop reading (inclusive)
   *                       or earlier if `max` events have already been read.
   */
  override def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int) =
    eventReader().ask(EventReader.ReadSync(fromSequenceNr, toSequenceNr, EventKeys.DefaultClassifier, max, Int.MaxValue, _ => true))(settings.readTimeout, self).mapTo[BatchReadResult]

  /**
   * Asynchronously batch-reads events whose `destinationAggregateIds` contains the given `aggregateId`. At most
   * `max` events must be returned that are within the sequence number bounds `fromSequenceNr` and `toSequenceNr`.
   *
   * @param fromSequenceNr sequence number to start reading (inclusive).
   * @param toSequenceNr   sequence number to stop reading (inclusive)
   *                       or earlier if `max` events have already been read.
   */
  override def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int, aggregateId: String) =
    eventReader().ask(EventReader.ReadSync(fromSequenceNr, toSequenceNr, numericStore.numericId(aggregateId, readOnly = true), max, Int.MaxValue, _ => true))(settings.readTimeout, self).mapTo[BatchReadResult]

  /**
   * Asynchronously batch-reads events from the raw event log. At most `max` events must be returned that are
   * within the sequence number bounds `fromSequenceNr` and `toSequenceNr` and that pass the given `filter`.
   *
   * @param fromSequenceNr sequence number to start reading (inclusive).
   * @param toSequenceNr   sequence number to stop reading (inclusive)
   *                       or earlier if `max` events have already been read.
   */
  override def replicationRead(fromSequenceNr: Long, toSequenceNr: Long, max: Int, scanLimit: Int, filter: (DurableEvent) => Boolean) =
    eventReader().ask(EventReader.ReadSync(fromSequenceNr, toSequenceNr, EventKeys.DefaultClassifier, max, scanLimit, filter))(settings.readTimeout, self).mapTo[BatchReadResult]

  private def readSync(fromSequenceNr: Long, toSequenceNr: Long, classifier: Int, max: Int, scanLimit: Int, filter: DurableEvent => Boolean): BatchReadResult = {

    val first = 1L max fromSequenceNr
    var last = first - 1L

    var scanned = 0
    var filtered = 0

    val builder = new VectorBuilder[DurableEvent]
    withIterator(first, classifier) { iter =>
      while (iter.hasNext && filtered < max && scanned < scanLimit) {
        val event = iter.next()
        if (filter(event)) {
          builder += event
          filtered += 1
        }
        scanned += 1
        last = event.localSequenceNr
      }

      BatchReadResult(builder.result(), last)
    }
  }

  /**
   * Asynchronously writes the replication progresses for source log ids given by `progresses` keys.
   */
  override def writeReplicationProgresses(progresses: Map[String, Long]) =
    completed(progressStore.writeProgresses(progresses))

  /**
   * Synchronously writes `events` to the given `partition`. The partition is calculated from the configured
   * `partitionSizeMax` and the current sequence number. Asynchronous writes will be supported in future versions.
   *
   * This method may only throw an exception if it can guarantee that `events` have not been written to the storage
   * backend. If this is not the case (e.g. after a timeout communicating with a remote storage backend) this method
   * must retry writing or give up by stopping the actor with `context.stop(self)`. This is necessary to avoid that
   * `events` are erroneously excluded from the event stream sent to event-sourced actors, views, writers and
   * processors, as they may later re-appear during recovery which would violate ordering/causality guarantees.
   *
   * Implementations that potentially retry a write for a longer time should use a [[CircuitBreaker]] for protecting
   * themselves against request overload.
   *
   * @see [[EventLogSettings]]
   */
  override def write(events: Seq[DurableEvent], partition: Long, clock: EventLogClock) =
    withBatch(batch => writeSync(events, clock, batch))

  private def writeSync(events: Seq[DurableEvent], clock: EventLogClock, batch: WriteBatch): Unit = {
    events.foreach { event =>
      val snr = event.localSequenceNr
      val value = eventBytes(event)
      batch.put(columnHandles.get(0), eventKeyBytes(EventKeys.DefaultClassifier, snr), value)
      event.destinationAggregateIds.foreach { id => // additionally index events by aggregate id
        batch.put(columnHandles.get(0), eventKeyBytes(numericStore.numericId(id), snr), value)
      }
    }

    updateCount += events.size
    if (updateCount >= settings.stateSnapshotLimit) {
      writeEventLogClockSnapshotSync(clock, batch)
      updateCount = 0
    }
  }

  /**
   * Synchronously writes metadata for a [[com.rbmhtechnology.eventuate.EventsourcingProtocol.Delete Delete]] request. This marks events up to
   * [[DeletionMetadata.toSequenceNr]] as deleted, i.e. they are not read on replay and indicates which remote logs
   * must have replicated these events before they are allowed to be physically deleted locally.
   */
  override def writeDeletionMetadata(data: DeletionMetadata) =
    deletionStore.writeDeletion(data)

  /**
   * Asynchronously writes the current snapshot of the event log clock
   */
  override def writeEventLogClockSnapshot(clock: EventLogClock) =
    withBatch(batch => completed(writeEventLogClockSnapshotSync(clock, batch)))

  private def readEventLogClockSnapshotSync: EventLogClock = {
    val clock = rocksdb.get(columnHandles.get(0), clockKeyBytes)
    if (null == clock) EventLogClock() else clockFromBytes(clock)
  }

  private def writeEventLogClockSnapshotSync(clock: EventLogClock, batch: WriteBatch): Unit =
    batch.put(columnHandles.get(0), clockKeyBytes, clockBytes(clock))

  /**
   * Instructs the log to asynchronously and physically delete events up to `toSequenceNr`. This operation completes when
   * physical deletion completed and returns the sequence nr up to which events have been deleted. This can be
   * smaller then the requested `toSequenceNr` if a backend has to keep events for internal reasons.
   * A backend that does not support physical deletion should not override this method.
   */
  override def delete(toSequenceNr: Long) = {
    import context.dispatcher
    val adjusted = readEventLogClockSnapshotSync.sequenceNr min toSequenceNr
    val promise = Promise[Unit]()
    context.actorOf(DeletionActor.props(rocksdb, writeOptions, columnHandles.get(0), settings.deletionBatchSize, toSequenceNr, promise))
    promise.future.map(_ => adjusted)
  }

  override def postStop(): Unit = {
    super.postStop()
    rocksdb.close()
  }

  private def withIterator[R](from: Long, classifier: Int)(body: EventIterator => R): R = {
    val iter = eventIterator(from, classifier)
    try {
      body(iter)
    } finally {
      iter.close()
    }
  }

  private def eventIterator(from: Long, classifier: Int): EventIterator =
    new EventIterator(from, classifier)

  private class EventIterator(from: Long, classifier: Int) extends Iterator[DurableEvent] with Closeable {
    val options = new ReadOptions().setVerifyChecksums(false).setPrefixSameAsStart(true).setSnapshot(rocksdb.getSnapshot)
    val iter1 = rocksdb.newIterator(columnHandles.get(0), options); iter1.seek(eventKeyBytes(classifier, from))
    val iter2 = new Iterator[(Array[Byte], Array[Byte])] {
      override def hasNext = iter1.isValid

      override def next() = {
        val res = (iter1.key(), iter1.value())
        iter1.next()
        res
      }
    }.takeWhile(entry => eventKeyFromBytes(entry._1).classifier == classifier).map(entry => eventFromBytes(entry._2))

    override def hasNext: Boolean = iter2.hasNext
    override def next(): DurableEvent = iter2.next()
    override def close(): Unit = {
      iter1.close()
      options.snapshot().close()
    }
  }

  private def eventReader(): ActorRef =
    context.actorOf(Props(new EventReader).withDispatcher("eventuate.log.dispatchers.read-dispatcher"))

  private class EventReader() extends Actor {
    import EventReader._

    def receive = {
      case ReadSync(from, to, classifier, max, scanLimit, filter) =>
        Try(readSync(from, to, classifier, max, scanLimit, filter)) match {
          case Success(r) => sender() ! r
          case Failure(e) => sender() ! Status.Failure(e)
        }
        context.stop(self)
    }
  }

  private object EventReader {
    case class ReadSync(fromSequenceNr: Long, toSequenceNr: Long, classifier: Int, max: Int, scanLimit: Int, filter: DurableEvent => Boolean)
  }
}
