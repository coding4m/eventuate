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

package com.rbmhtechnology.eventuate.log.leveldb

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.pattern.ask
import akka.serialization.SerializationExtension

import com.rbmhtechnology.eventuate.DurableEvent
import com.rbmhtechnology.eventuate.log._
import com.typesafe.config.Config

import org.fusesource.leveldbjni.JniDBFactory._
import org.iq80.leveldb._

import scala.collection.JavaConverters._
import scala.collection.immutable._
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util._

class LeveldbEventLogSettings(config: Config) extends EventLogSettings {
  val readTimeout: FiniteDuration =
    config.getDuration("eventuate.log.read-timeout", TimeUnit.MILLISECONDS).millis

  val prefix: String =
    config.getString("eventuate.log.leveldb.prefix")

  val rootDir: String =
    config.getString("eventuate.log.leveldb.dir")

  val fsync: Boolean =
    config.getBoolean("eventuate.log.leveldb.fsync")

  val stateSnapshotLimit: Int =
    config.getInt("eventuate.log.leveldb.state-snapshot-limit")

  val deletionBatchSize: Int =
    config.getInt("eventuate.log.leveldb.deletion-batch-size")

  val initRetryDelay: FiniteDuration =
    Duration.Zero

  val initRetryMax: Int =
    0

  val deletionRetryDelay: FiniteDuration =
    config.getDuration("eventuate.log.leveldb.deletion-retry-delay", TimeUnit.MILLISECONDS).millis

  val partitionSize: Long =
    Long.MaxValue
}

/**
 * [[LeveldbEventLog]] actor state.
 */
case class LeveldbEventLogState(eventLogClock: EventLogClock, deletionMetadata: DeletionMetadata) extends EventLogState

/**
 * An event log actor with LevelDB as storage backend. The directory containing the LevelDB files
 * for this event log is named after the constructor parameters using the template "`prefix`-`id`"
 * and stored in a root directory defined by the `log.leveldb.dir` configuration.
 *
 * '''Please note:''' `prefix` and `id` are currently not escaped when creating the directory name.
 *
 * @param id unique log id.
 */
class LeveldbEventLog(id: String) extends EventLog[LeveldbEventLogState](id) with LeveldbBatchLayer {
  import LeveldbEventLog._

  override val settings = new LeveldbEventLogSettings(context.system.settings.config)
  private val serialization = SerializationExtension(context.system)

  private val leveldbDir = new File(settings.rootDir, s"${settings.prefix}_$id"); leveldbDir.mkdirs()
  private val leveldbOptions = new Options().createIfMissing(true)
  protected val leveldb = factory.open(leveldbDir, leveldbOptions)
  protected val writeOptions = new WriteOptions().sync(settings.fsync).snapshot(false)
  private def readOptions = new ReadOptions().verifyChecksums(false)
  private def snapshotOptions(): ReadOptions = readOptions.snapshot(leveldb.getSnapshot)

  private val numericStore = new NumericIdStore(leveldb, writeOptions, -1)
  private val progressStore = new ProgressStore(leveldb, writeOptions, -2)
  private val deletionStore = new DeletionStore(leveldb, writeOptions, -3)

  private var updateCount: Long = 0L

  def logDir: File =
    leveldbDir

  override def write(events: Seq[DurableEvent], partition: Long, clock: EventLogClock): Unit =
    withBatch(batch => writeSync(events, clock, batch))

  override def writeReplicationProgresses(progresses: Map[String, Long]): Future[Unit] =
    completed(progressStore.writeProgresses(progresses))

  def writeEventLogClockSnapshot(clock: EventLogClock): Future[Unit] =
    withBatch(batch => Future.fromTry(Try(writeEventLogClockSnapshotSync(clock, batch))))

  private def eventIterator(from: Long, classifier: Long): EventIterator =
    new EventIterator(from, classifier)

  private def eventReader(): ActorRef =
    context.actorOf(Props(new EventReader).withDispatcher("eventuate.log.dispatchers.read-dispatcher"))

  override def readReplicationProgresses: Future[Map[String, Long]] =
    completed(progressStore.readProgresses())

  override def readReplicationProgress(logId: String): Future[Long] =
    completed(progressStore.readProgress(logId))

  override def replicationRead(fromSequenceNr: Long, toSequenceNr: Long, max: Int, scanLimit: Int, filter: DurableEvent => Boolean): Future[BatchReadResult] =
    eventReader().ask(EventReader.ReadSync(fromSequenceNr, toSequenceNr, EventKey.DefaultClassifier, max, scanLimit, filter))(settings.readTimeout, self).mapTo[BatchReadResult]

  override def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int): Future[BatchReadResult] =
    eventReader().ask(EventReader.ReadSync(fromSequenceNr, toSequenceNr, EventKey.DefaultClassifier, max, Int.MaxValue, _ => true))(settings.readTimeout, self).mapTo[BatchReadResult]

  override def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int, aggregateId: String): Future[BatchReadResult] =
    eventReader().ask(EventReader.ReadSync(fromSequenceNr, toSequenceNr, numericStore.numericId(aggregateId, readOnly = true), max, Int.MaxValue, _ => true))(settings.readTimeout, self).mapTo[BatchReadResult]

  override def recoverState: Future[LeveldbEventLogState] = completed {
    val clockSnapshot = readEventLogClockSnapshot
    val clockRecovered = withEventIterator(clockSnapshot.sequenceNr + 1L, EventKey.DefaultClassifier) { iter =>
      iter.foldLeft(clockSnapshot)(_ update _)
    }
    LeveldbEventLogState(clockRecovered, deletionStore.readDeletion())
  }

  override def writeDeletionMetadata(deleteMetadata: DeletionMetadata) =
    deletionStore.writeDeletion(deleteMetadata)

  override def delete(toSequenceNr: Long): Future[Long] = {
    val adjusted = readEventLogClockSnapshot.sequenceNr min toSequenceNr
    val promise = Promise[Unit]()
    spawnDeletionActor(adjusted, promise)
    promise.future.map(_ => adjusted)(context.dispatcher)
  }

  private def spawnDeletionActor(toSequenceNr: Long, promise: Promise[Unit]): ActorRef =
    context.actorOf(DeletionActor.props(leveldb, readOptions, writeOptions, settings.deletionBatchSize, toSequenceNr, promise))

  private def readEventLogClockSnapshot: EventLogClock = {
    leveldb.get(clockKeyBytes) match {
      case null => EventLogClock()
      case cval => clockFromBytes(cval)
    }
  }

  private def readSync(fromSequenceNr: Long, toSequenceNr: Long, classifier: Long, max: Int, scanLimit: Int, filter: DurableEvent => Boolean): BatchReadResult = {
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

  private def writeSync(events: Seq[DurableEvent], clock: EventLogClock, batch: WriteBatch): Unit = {
    events.foreach { event =>
      val sequenceNr = event.localSequenceNr
      val eventBytes = this.eventBytes(event)
      batch.put(eventKeyBytes(EventKey.DefaultClassifier, sequenceNr), eventBytes)
      event.destinationAggregateIds.foreach { id => // additionally index events by aggregate id
        batch.put(eventKeyBytes(numericStore.numericId(id), sequenceNr), eventBytes)
      }
    }

    updateCount += events.size
    if (updateCount >= settings.stateSnapshotLimit) {
      writeEventLogClockSnapshotSync(clock, batch)
      updateCount = 0
    }
  }

  private def writeEventLogClockSnapshotSync(clock: EventLogClock, batch: WriteBatch): Unit =
    batch.put(clockKeyBytes, clockBytes(clock))

  private def withEventIterator[R](from: Long, classifier: Long)(body: EventIterator => R): R = {
    val iter = eventIterator(from, classifier)
    try {
      body(iter)
    } finally {
      iter.close()
    }
  }

  private class EventIterator(from: Long, classifier: Long) extends Iterator[DurableEvent] with Closeable {
    val options = snapshotOptions()
    val iter1 = leveldb.iterator(options)
    val iter2 = iter1.asScala.takeWhile(entry => eventKey(entry.getKey).classifier == classifier && !eventKeyEndBytes.sameElements(entry.getKey)).map(entry => event(entry.getValue))
    iter1.seek(eventKeyBytes(classifier, from))
    override def hasNext: Boolean = iter2.hasNext
    override def next(): DurableEvent = iter2.next()
    override def close(): Unit = {
      iter1.close()
      options.snapshot().close()
    }
  }

  private def eventBytes(e: DurableEvent): Array[Byte] =
    serialization.serialize(e).get

  private def event(a: Array[Byte]): DurableEvent =
    serialization.deserialize(a, classOf[DurableEvent]).get

  private def clockBytes(clock: EventLogClock): Array[Byte] =
    serialization.serialize(clock).get

  private def clockFromBytes(a: Array[Byte]): EventLogClock =
    serialization.deserialize(a, classOf[EventLogClock]).get

  override def preStart(): Unit = {
    leveldb.put(eventKeyEndBytes, Array.empty[Byte])
    super.preStart()
  }

  override def postStop(): Unit = {
    // Leveldb iterators that are used by threads other that this actor's dispatcher threads
    // are used in child actors of this actor. This ensures that these iterators are closed
    // before this actor closes the leveldb instance (fixing issue #234).
    leveldb.close()
    super.postStop()
  }

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
    case class ReadSync(fromSequenceNr: Long, toSequenceNr: Long, classifier: Long, max: Int, scanLimit: Int, filter: DurableEvent => Boolean)
  }
}

object LeveldbEventLog {
  private[leveldb]type CloseableIterator[A] = Iterator[A] with Closeable

  private[leveldb] case class EventKey(classifier: Long, sequenceNr: Long)

  private[leveldb] object EventKey {
    val DefaultClassifier: Long = 0L
  }

  private[leveldb] def eventKeyBytes(classifier: Long, sequenceNr: Long): Array[Byte] = {
    val bb = ByteBuffer.allocate(16)
    bb.putLong(classifier)
    bb.putLong(sequenceNr)
    bb.array
  }

  private[leveldb] def eventKey(a: Array[Byte]): EventKey = {
    val bb = ByteBuffer.wrap(a)
    EventKey(bb.getLong, bb.getLong)
  }

  private[leveldb] val eventKeyEnd: EventKey =
    EventKey(Long.MaxValue, Long.MaxValue)

  private[leveldb] val eventKeyEndBytes: Array[Byte] =
    eventKeyBytes(eventKeyEnd.classifier, eventKeyEnd.sequenceNr)

  private[leveldb] val clockKeyBytes: Array[Byte] =
    eventKeyBytes(0L, 0L)

  private[leveldb] def completed[A](body: => A): Future[A] =
    Future.fromTry(Try(body))

  /**
   * Creates a [[LeveldbEventLog]] configuration object.
   *
   * @param logId unique log id.
   * @param batching `true` if write-batching shall be enabled (recommended).
   */
  def props(logId: String, batching: Boolean = true): Props = {
    val logProps = Props(new LeveldbEventLog(logId)).withDispatcher("eventuate.log.dispatchers.write-dispatcher")
    if (batching) Props(new BatchingLayer(logProps)) else logProps
  }
}
