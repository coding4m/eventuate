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

package com.rbmhtechnology.eventuate.snapshot.leveldb

import java.io.File

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import com.rbmhtechnology.eventuate.Snapshot
import com.rbmhtechnology.eventuate.snapshot.SnapshotStore
import org.fusesource.leveldbjni.JniDBFactory.factory
import org.iq80.leveldb.{ Options, ReadOptions, WriteBatch, WriteOptions }

import scala.annotation.tailrec
import scala.concurrent.Future

/**
 * @author siuming
 */
class LeveldbSnapshotStore(system: ActorSystem, id: String) extends SnapshotStore(system, id) {

  private implicit val serialization = SerializationExtension(system)
  private val settings = new LeveldbSnapshotSettings(system)

  private val leveldbDir = new File(settings.rootDir, s"${settings.prefix}_$id"); leveldbDir.mkdirs()
  private val leveldbOptions = new Options().createIfMissing(true)
  private val leveldb = factory.open(leveldbDir, leveldbOptions)
  private val writeOptions = new WriteOptions().sync(settings.fsync).snapshot(false)
  private def readOptions = new ReadOptions().verifyChecksums(false).snapshot(leveldb.getSnapshot)
  private def snapshotOptions = readOptions.snapshot(leveldb.getSnapshot)

  /**
   * Asynchronously loads the latest snapshot saved by an event-sourced actor, view, writer or processor
   * identified by `emitterId`.
   */
  override def load(emitterId: String) = {
    import settings.readDispatcher
    Future {
      withIterator[Option[Snapshot]](snapshotOptions, reserved = true) { it =>
        it.last(emitterId).takeWhile(_.emitterId == emitterId).find(_.emitterId == emitterId).map(_.snapshot)
      }
    }
  }

  /**
   * Asynchronously saves the given `snapshot`.
   */
  override def save(snapshot: Snapshot) = {
    import settings.writeDispatcher
    Future {
      withIterator[Unit](snapshotOptions, reserved = true) { it =>
        withBatch { batch =>
          val key = SnapshotItem.itemKey(snapshot.metadata.emitterId, snapshot.metadata.sequenceNr)
          val value = SnapshotItem.itemValue(snapshot)
          batch.put(key, value)
          it.seek(key)
            .takeWhile(_.emitterId == snapshot.metadata.emitterId)
            .drop(settings.snapshotsPerMax - 1)
            .foreach(it => batch.delete(it.key))
        }
      }
    }
  }

  override def delete(emitterId: String) = {
    import settings.writeDispatcher
    Future {
      withIterator(snapshotOptions, reserved = false) { it =>
        deleteBatch(it.first(emitterId).takeWhile(_.emitterId == emitterId).map(_.key))
      }
    }
  }

  /**
   * Asynchronously deletes all snapshots with a sequence number greater than or equal `lowerSequenceNr`.
   */
  override def delete(lowerSequenceNr: Long) = {
    import settings.writeDispatcher
    Future {
      withIterator(snapshotOptions, reserved = false) { it =>
        deleteBatch(it.seekToFirst().filter(_.sequenceNr >= lowerSequenceNr).map(_.key))
      }
    }
  }

  @tailrec
  private def deleteBatch(it: Iterator[Array[Byte]]): Unit = {
    withBatch(batch => it.take(settings.deletionBatchSize).foreach(batch.delete))
    if (it.hasNext) {
      deleteBatch(it)
    }
  }

  override def close() = {
    leveldb.close()
  }

  private def withBatch[R](body: WriteBatch => R): R = {
    val batch = leveldb.createWriteBatch()
    try {
      val r = body(batch)
      leveldb.write(batch, writeOptions)
      r
    } finally {
      batch.close()
    }
  }

  private def withIterator[T](options: ReadOptions, reserved: Boolean)(body: SnapshotIterator => T): T = {
    val iterator = SnapshotIterator(leveldb.iterator(options), reserved)
    try {
      body(iterator)
    } finally {
      iterator.close()
      options.snapshot().close()
    }
  }
}
