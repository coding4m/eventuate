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

import akka.actor.{ Actor, PoisonPill, Props }
import org.rocksdb.{ ReadOptions, RocksDB, WriteOptions }

import scala.annotation.tailrec
import scala.concurrent.Promise

private object RocksdbDeletionActor {
  case object DeleteBatch

  def props(rocksdb: RocksDB, rocksdbReadOptions: ReadOptions, rocksdbWriteOptions: WriteOptions, batchSize: Int, toSequenceNr: Long, promise: Promise[Unit]): Props =
    Props(new RocksdbDeletionActor(rocksdb, rocksdbReadOptions, rocksdbWriteOptions, batchSize, toSequenceNr, promise))
}

private class RocksdbDeletionActor(
  val rocksdb: RocksDB,
  val rocksdbReadOptions: ReadOptions,
  val rocksdbWriteOptions: WriteOptions,
  batchSize: Int,
  toSequenceNr: Long,
  promise: Promise[Unit])
  extends Actor with RocksdbBatchLayer {

  import RocksdbEventLog._
  import RocksdbDeletionActor._

  val eventKeyIterator: CloseableIterator[EventKey] = newEventKeyIterator

  override def preStart() = self ! DeleteBatch

  override def postStop() = eventKeyIterator.close()

  override def receive = {
    case DeleteBatch =>
      withBatch { batch =>
        eventKeyIterator.take(batchSize).foreach { eventKey =>
          batch.remove(eventKeyBytes(eventKey.classifier, eventKey.sequenceNr))
        }
      }
      if (eventKeyIterator.hasNext) {
        self ! DeleteBatch
      } else {
        promise.success(())
        self ! PoisonPill
      }
  }

  private def newEventKeyIterator: CloseableIterator[EventKey] = {
    new Iterator[EventKey] with Closeable {
      val iterator = rocksdb.newIterator(rocksdbReadOptions)
      iterator.seek(eventKeyBytes(EventKey.DefaultClassifier, 1L))

      @tailrec
      override def hasNext: Boolean = {
        val key = if (iterator.isValid) eventKeyFromBytes(iterator.key()) else eventKeyEnd
        key != eventKeyEnd &&
          (key.sequenceNr <= toSequenceNr || {
            iterator.seek(eventKeyBytes(key.classifier + 1, 1L))
            hasNext
          })
      }

      override def next() = {
        val res = eventKeyFromBytes(iterator.key())
        iterator.next()
        res
      }
      override def close() = {
        iterator.close()
        rocksdbReadOptions.snapshot().close()
      }
    }
  }
}
