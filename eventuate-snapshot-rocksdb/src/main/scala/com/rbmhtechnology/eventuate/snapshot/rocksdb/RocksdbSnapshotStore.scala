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

package com.rbmhtechnology.eventuate.snapshot.rocksdb

import java.nio.file.{ Files, Paths }

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import com.rbmhtechnology.eventuate.Snapshot
import com.rbmhtechnology.eventuate.snapshot.SnapshotStore
import org.rocksdb._

import scala.concurrent.Future

/**
 * @author siuming
 */
class RocksdbSnapshotStore(system: ActorSystem, id: String) extends SnapshotStore(system, id) {

  private implicit val serialization = SerializationExtension(system)
  private val settings = new RocksdbSnapshotSettings(system)

  private val rocksdbDir = Paths.get(settings.rootDir, s"${settings.prefix}_$id"); Files.createDirectories(rocksdbDir)
  private val rocksdbOptions = new Options().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
  protected val rocksdbWriteOptions = new WriteOptions().setSync(settings.fsync)
  protected val rocksdb = RocksDB.open(rocksdbOptions, rocksdbDir.toAbsolutePath.toString)

  /**
   * Asynchronously loads the latest snapshot saved by an event-sourced actor, view, writer or processor
   * identified by `emitterId`.
   */
  override def load(emitterId: String) = {
    import settings.readDispatcher
    Future {
      withIterator[Option[Snapshot]](newIterator(), reserved = true) { it =>
        it.lastForPrev(emitterId).find(_.emitterId == emitterId).map(_.snapshot)
      }
    }
  }

  /**
   * Asynchronously saves the given `snapshot`.
   */
  override def save(snapshot: Snapshot) = {
    import settings.writeDispatcher
    Future {
      withIterator[Unit](newIterator(), reserved = true) { it =>
        val batch = new WriteBatch()
        val key = SnapshotItem.itemKey(snapshot.metadata.emitterId, snapshot.metadata.sequenceNr)
        val value = SnapshotItem.itemValue(snapshot)
        batch.put(key, value)
        it.seekForPrev(key)
          .drop(settings.snapshotsPerMax - 1)
          .takeWhile(_.emitterId == snapshot.metadata.emitterId)
          .foreach(it => batch.remove(it.key))

        rocksdb.write(rocksdbWriteOptions, batch)
      }
    }
  }

  /**
   * Asynchronously deletes all snapshots with `emitterId`.
   */
  override def delete(emitterId: String) = {
    import settings.writeDispatcher
    Future {
      withIterator[Unit](newIterator(), reserved = false) { it =>
        val batch = new WriteBatch()
        it.first(emitterId).takeWhile(_.emitterId == emitterId).foreach(item => batch.remove(item.key))
        rocksdb.write(rocksdbWriteOptions, batch)
      }
    }
  }

  /**
   * Asynchronously deletes all snapshots with a sequence number greater than or equal `lowerSequenceNr`.
   */
  override def delete(lowerSequenceNr: Long) = {
    // todo use batch
    import settings.writeDispatcher
    Future {
      withIterator[Unit](newIterator(), reserved = false) { it =>
        it.seekToFirst().filter(_.sequenceNr >= lowerSequenceNr).foreach(item => rocksdb.delete(item.key))
      }
    }
  }

  private def newIterator(): RocksIterator = {
    rocksdb.newIterator(new ReadOptions().setSnapshot(rocksdb.getSnapshot))
  }

  private def withIterator[T](it: RocksIterator, reserved: Boolean)(body: SnapshotIterator => T): T = {
    val sit = SnapshotIterator(it, reserved)
    try {
      body(sit)
    } finally {
      sit.close()
    }
  }
}
