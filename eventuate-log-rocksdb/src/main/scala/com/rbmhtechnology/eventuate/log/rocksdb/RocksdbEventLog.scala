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
import java.nio.ByteBuffer
import java.nio.file.{ Files, Paths }
import java.util.concurrent.TimeUnit

import akka.actor.{ Actor, ActorRef, Props, Status }
import akka.pattern._
import akka.serialization.SerializationExtension
import com.rbmhtechnology.eventuate.DurableEvent
import com.rbmhtechnology.eventuate.log._
import com.typesafe.config.Config
import org.rocksdb._

import scala.collection.immutable.{ Seq, VectorBuilder }
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

/**
 * @author siuming
 */
class RocksdbEventLogSettings(config: Config) extends EventLogSettings {

  val readTimeout: FiniteDuration =
    config.getDuration("eventuate.log.read-timeout", TimeUnit.MILLISECONDS).millis

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

  private[rocksdb]type CloseableIterator[A] = Iterator[A] with Closeable

  private[rocksdb] case class EventKey(classifier: Int, sequenceNr: Long)

  private[rocksdb] object EventKey {
    val DefaultClassifier: Int = 0
  }

  private val clockKeyBytes: Array[Byte] =
    eventKeyBytes(0, 0L)

  private[rocksdb] val eventKeyEnd: EventKey =
    EventKey(Int.MaxValue, Long.MaxValue)

  private val eventKeyEndBytes: Array[Byte] =
    eventKeyBytes(eventKeyEnd.classifier, eventKeyEnd.sequenceNr)

  private[rocksdb] def eventKeyBytes(classifier: Int, sequenceNr: Long): Array[Byte] = {
    val bb = ByteBuffer.allocate(12)
    bb.putInt(classifier)
    bb.putLong(sequenceNr)
    bb.array
  }

  private[rocksdb] def eventKey(a: Array[Byte]): EventKey = {
    val bb = ByteBuffer.wrap(a)
    EventKey(bb.getInt, bb.getLong)
  }

  private[rocksdb] def longBytes(l: Long): Array[Byte] =
    ByteBuffer.allocate(8).putLong(l).array

  private[rocksdb] def longFromBytes(a: Array[Byte]): Long =
    ByteBuffer.wrap(a).getLong

  private def completed[A](body: => A): Future[A] =
    Future.fromTry(Try(body))

  /**
   * Creates a [[RocksdbEventLog]] configuration object.
   *
   * @param logId unique log id.
   * @param prefix prefix of the directory that contains the LevelDB files.
   * @param batching `true` if write-batching shall be enabled (recommended).
   */
  def props(logId: String, prefix: String = "log", batching: Boolean = true): Props = {
    val logProps = Props(new RocksdbEventLog(logId, prefix)).withDispatcher("eventuate.log.dispatchers.write-dispatcher")
    if (batching) Props(new BatchingLayer(logProps)) else logProps
  }
}
class RocksdbEventLog(id: String, prefix: String) extends EventLog[RocksdbEventLogState](id) with RocksdbBatchLayer {

  import RocksdbEventLog._

  override val settings = new RocksdbEventLogSettings(context.system.settings.config)
  private val serialization = SerializationExtension(context.system)

  private val rocksdbDir = Paths.get(settings.rootDir, s"$prefix-$id"); Files.createDirectories(rocksdbDir)
  private val rocksdbOptions = new Options().setCreateIfMissing(true)
  protected val rocksdbWriteOptions = new WriteOptions().setSync(settings.fsync)
  protected val rocksdb = RocksDB.open(rocksdbOptions, rocksdbDir.toAbsolutePath.toString)

  private val aggregateIdMap = new RocksdbNumericIdentifierStore(rocksdb, -1)
  private val eventLogIdMap = new RocksdbNumericIdentifierStore(rocksdb, -2)
  private val replicationProgressMap = new RocksdbReplicationProgressStore(rocksdb, -3, eventLogIdMap.numericId, eventLogIdMap.findId)
  private val deletionMetadataStore = new RocksdbDeletionMetadataStore(rocksdb, rocksdbWriteOptions, -4)

  private var updateCount: Long = 0L

  override def preStart(): Unit = {
    withIterator(iter => aggregateIdMap.readIdMap(iter))
    withIterator(iter => eventLogIdMap.readIdMap(iter))
    rocksdb.put(eventKeyEndBytes, Array.empty[Byte])
    super.preStart()
  }

  /**
   * Asynchronously recovers event log state from the storage backend.
   */
  override def recoverState = completed {
    val clockSnapshot = readEventLogClockSnapshotSync
    val clockRecovered = withEventIterator(clockSnapshot.sequenceNr + 1L, EventKey.DefaultClassifier) { iter =>
      iter.foldLeft(clockSnapshot)(_ update _)
    }
    RocksdbEventLogState(clockRecovered, deletionMetadataStore.readDeletionMetadata())
  }

  /**
   * Asynchronously reads all stored local replication progresses.
   *
   * @see [[com.rbmhtechnology.eventuate.ReplicationProtocol.GetReplicationProgresses]]
   */
  override def readReplicationProgresses =
    completed(withIterator(iter => replicationProgressMap.readReplicationProgresses(iter)))

  /**
   * Asynchronously reads the replication progress for given source `logId`.
   *
   * @see [[com.rbmhtechnology.eventuate.ReplicationProtocol.GetReplicationProgress]]
   */
  override def readReplicationProgress(logId: String) =
    completed(withIterator(_ => replicationProgressMap.readReplicationProgress(logId)))

  /**
   * Asynchronously batch-reads events from the raw event log. At most `max` events must be returned that are
   * within the sequence number bounds `fromSequenceNr` and `toSequenceNr`.
   *
   * @param fromSequenceNr sequence number to start reading (inclusive).
   * @param toSequenceNr   sequence number to stop reading (inclusive)
   *                       or earlier if `max` events have already been read.
   */
  override def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int) =
    eventReader().ask(EventReader.ReadSync(fromSequenceNr, toSequenceNr, EventKey.DefaultClassifier, max))(settings.readTimeout, self).mapTo[BatchReadResult]

  /**
   * Asynchronously batch-reads events whose `destinationAggregateIds` contains the given `aggregateId`. At most
   * `max` events must be returned that are within the sequence number bounds `fromSequenceNr` and `toSequenceNr`.
   *
   * @param fromSequenceNr sequence number to start reading (inclusive).
   * @param toSequenceNr   sequence number to stop reading (inclusive)
   *                       or earlier if `max` events have already been read.
   */
  override def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int, aggregateId: String) =
    eventReader().ask(EventReader.ReadSync(fromSequenceNr, toSequenceNr, aggregateIdMap.numericId(aggregateId), max))(settings.readTimeout, self).mapTo[BatchReadResult]

  /**
   * Asynchronously batch-reads events from the raw event log. At most `max` events must be returned that are
   * within the sequence number bounds `fromSequenceNr` and `toSequenceNr` and that pass the given `filter`.
   *
   * @param fromSequenceNr sequence number to start reading (inclusive).
   * @param toSequenceNr   sequence number to stop reading (inclusive)
   *                       or earlier if `max` events have already been read.
   */
  override def replicationRead(fromSequenceNr: Long, toSequenceNr: Long, max: Int, scanLimit: Int, filter: (DurableEvent) => Boolean) =
    eventReader().ask(EventReader.ReadIteratorSync(fromSequenceNr, toSequenceNr, EventKey.DefaultClassifier, max, scanLimit, filter))(settings.readTimeout, self).mapTo[BatchReadResult]

  private def readSync(fromSequenceNr: Long, toSequenceNr: Long, classifier: Int, maxNr: Int): BatchReadResult = {
    import scala.collection.JavaConversions._

    val first = 1L max fromSequenceNr
    val last = (toSequenceNr - fromSequenceNr) min maxNr
    val opts = new ReadOptions().setVerifyChecksums(false).setSnapshot(rocksdb.getSnapshot)
    try {
      val readKeys = for (sequenceNr <- first to last) yield eventKeyBytes(classifier, sequenceNr)
      val readEvents = rocksdb.multiGet(opts, readKeys).map(kv => event(kv._2)).toSeq.sortWith((e1, e2) => e1.localSequenceNr < e2.localSequenceNr)
      BatchReadResult(Seq(readEvents: _*), readEvents.lastOption.map(_.localSequenceNr).getOrElse(first - 1))
    } finally {
      opts.snapshot().close()
    }

  }

  private def readIteratorSync(fromSequenceNr: Long, toSequenceNr: Long, classifier: Int, max: Int, scanLimit: Int, filter: DurableEvent => Boolean): BatchReadResult = {
    val builder = new VectorBuilder[DurableEvent]

    val first = 1L max fromSequenceNr
    var last = first - 1L

    var scanned = 0
    var filtered = 0

    withEventIterator(first, classifier) { iter =>
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
    completed(withBatch(batch => progresses.foreach(p => replicationProgressMap.writeReplicationProgress(p._1, p._2, batch))))

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
      val sequenceNr = event.localSequenceNr
      val eventBytes = this.eventBytes(event)
      batch.put(eventKeyBytes(EventKey.DefaultClassifier, sequenceNr), eventBytes)
      event.destinationAggregateIds.foreach { id => // additionally index events by aggregate id
        batch.put(eventKeyBytes(aggregateIdMap.numericId(id), sequenceNr), eventBytes)
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
    deletionMetadataStore.writeDeletionMetadata(data)

  /**
   * Asynchronously writes the current snapshot of the event log clock
   */
  override def writeEventLogClockSnapshot(clock: EventLogClock) =
    withBatch(batch => Future.fromTry(Try(writeEventLogClockSnapshotSync(clock, batch))))

  private def readEventLogClockSnapshotSync: EventLogClock = {
    rocksdb.get(clockKeyBytes) match {
      case null => EventLogClock()
      case cval => clock(cval)
    }
  }

  private def writeEventLogClockSnapshotSync(clock: EventLogClock, batch: WriteBatch): Unit =
    batch.put(clockKeyBytes, clockBytes(clock))

  /**
   * Instructs the log to asynchronously and physically delete events up to `toSequenceNr`. This operation completes when
   * physical deletion completed and returns the sequence nr up to which events have been deleted. This can be
   * smaller then the requested `toSequenceNr` if a backend has to keep events for internal reasons.
   * A backend that does not support physical deletion should not override this method.
   */
  override def delete(toSequenceNr: Long) = {
    val adjusted = readEventLogClockSnapshotSync.sequenceNr min toSequenceNr
    val promise = Promise[Unit]()
    spawnDeletionActor(adjusted, promise)
    promise.future.map(_ => adjusted)(context.dispatcher)
  }

  private def spawnDeletionActor(toSequenceNr: Long, promise: Promise[Unit]): ActorRef =
    context.actorOf(RocksdbDeletionActor.props(rocksdb, new ReadOptions().setVerifyChecksums(false), rocksdbWriteOptions, settings.deletionBatchSize, toSequenceNr, promise))

  override def postStop(): Unit = {
    rocksdb.close()
    super.postStop()
  }

  private def eventBytes(e: DurableEvent): Array[Byte] =
    serialization.serialize(e).get

  private def event(a: Array[Byte]): DurableEvent =
    serialization.deserialize(a, classOf[DurableEvent]).get

  private def clockBytes(clock: EventLogClock): Array[Byte] =
    serialization.serialize(clock).get

  private def clock(a: Array[Byte]): EventLogClock =
    serialization.deserialize(a, classOf[EventLogClock]).get

  private def withIterator[R](body: RocksIterator => R): R = {
    val so = new ReadOptions().setVerifyChecksums(false).setSnapshot(rocksdb.getSnapshot)
    val iter = rocksdb.newIterator(so)
    try {
      body(iter)
    } finally {
      iter.close()
      so.snapshot().close()
    }
  }

  private def withEventIterator[R](from: Long, classifier: Int)(body: EventIterator => R): R = {
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
    val opts = new ReadOptions().setVerifyChecksums(false).setSnapshot(rocksdb.getSnapshot)

    val iter1 = rocksdb.newIterator(opts); iter1.seek(eventKeyBytes(classifier, from))
    val iter2 = new Iterator[(Array[Byte], Array[Byte])] {
      override def hasNext = iter1.isValid

      override def next() = {
        iter1.next()
        (iter1.key(), iter1.value())
      }
    }.takeWhile(entry => eventKey(entry._1).classifier == classifier).map(entry => event(entry._2))

    override def hasNext: Boolean =
      iter2.hasNext

    override def next(): DurableEvent =
      iter2.next()

    override def close(): Unit = {
      iter1.close()
      opts.snapshot().close()
    }
  }

  private def eventReader(): ActorRef =
    context.actorOf(Props(new EventReader).withDispatcher("eventuate.log.dispatchers.read-dispatcher"))

  private class EventReader() extends Actor {
    import EventReader._

    def receive = {
      case ReadSync(from, to, classifier, max) =>
        Try(readSync(from, to, classifier, max)) match {
          case Success(r) => sender() ! r
          case Failure(e) => sender() ! Status.Failure(e)
        }
        context.stop(self)

      case ReadIteratorSync(from, to, classifier, max, scanLimit, filter) =>
        Try(readIteratorSync(from, to, classifier, max, scanLimit, filter)) match {
          case Success(r) => sender() ! r
          case Failure(e) => sender() ! Status.Failure(e)
        }
        context.stop(self)
    }
  }

  private object EventReader {
    case class ReadSync(fromSequenceNr: Long, toSequenceNr: Long, classifier: Int, max: Int)
    case class ReadIteratorSync(fromSequenceNr: Long, toSequenceNr: Long, classifier: Int, max: Int, scanLimit: Int, filter: DurableEvent => Boolean)
  }
}
