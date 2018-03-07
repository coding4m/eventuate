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
import org.rocksdb.{ ColumnFamilyHandle, ReadOptions, RocksDB, WriteOptions }

import scala.annotation.tailrec
import scala.concurrent.Promise

private object DeletionActor {
  case object DeleteBatch
  trait DeletionIterator extends Iterator[EventKey] with Closeable

  def props(
    rocksdb: RocksDB,
    writeOptios: WriteOptions,
    columnHandle: ColumnFamilyHandle,
    deleteBatch: Int,
    deleteTo: Long,
    promise: Promise[Unit]): Props =
    Props(new DeletionActor(rocksdb, writeOptios, columnHandle, deleteBatch, deleteTo, promise))
}
private class DeletionActor(
  val rocksdb: RocksDB,
  val writeOptions: WriteOptions,
  columnHandle: ColumnFamilyHandle,
  deleteBatch: Int,
  deleteTo: Long,
  promise: Promise[Unit]) extends Actor with RocksdbBatchLayer {

  import EventKeys._
  import DeletionActor._

  val iterator: DeletionIterator = newIterator()

  override def receive = {
    case DeleteBatch =>
      withBatch { batch =>
        iterator.take(deleteBatch).foreach { eventKey =>
          batch.remove(columnHandle, eventKeyBytes(eventKey.classifier, eventKey.sequenceNr))
        }
      }
      if (iterator.hasNext) self ! DeleteBatch
      else promise.success(())
  }

  override def preStart() = {
    import context.dispatcher
    self ! DeleteBatch
    promise.future.onComplete(_ => self ! PoisonPill)
  }

  override def postStop() = iterator.close()

  private def newIterator(): DeletionIterator = new DeletionIterator {
    val options = new ReadOptions().setVerifyChecksums(false).setPrefixSameAsStart(false).setSnapshot(rocksdb.getSnapshot)
    val iterator = rocksdb.newIterator(columnHandle, options)
    iterator.seek(eventKeyBytes(EventKeys.DefaultClassifier, 1L))

    @tailrec
    override def hasNext: Boolean = {
      val key = if (iterator.isValid) eventKeyFromBytes(iterator.key()) else eventKeyEnd
      key != eventKeyEnd &&
        (key.sequenceNr <= deleteTo || {
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
      options.snapshot().close()
    }
  }
}
