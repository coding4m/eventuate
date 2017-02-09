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

import java.nio.ByteBuffer

import org.rocksdb.{ RocksDB, RocksIterator, WriteBatch }

import scala.annotation.tailrec

private class RocksdbReplicationProgressStore(rocksdb: RocksDB, classifier: Int, numericId: String => Int, findId: Int => Option[String]) {
  private val rpKeyEnd: Int =
    Int.MaxValue

  private val rpKeyEndBytes: Array[Byte] =
    rpKeyBytes(rpKeyEnd)

  rocksdb.put(rpKeyEndBytes, Array.empty[Byte])

  def writeReplicationProgress(logId: String, logSnr: Long, batch: WriteBatch): Unit = {
    val nid = numericId(logId)
    batch.put(rpKeyBytes(nid), RocksdbEventLog.longBytes(logSnr))
  }

  def readReplicationProgress(logId: String): Long = {
    val nid = numericId(logId)
    val progress = rocksdb.get(rpKeyBytes(nid))
    if (progress == null) 0L else RocksdbEventLog.longFromBytes(progress)
  }

  def readReplicationProgresses(iter: RocksIterator): Map[String, Long] = {
    iter.seek(rpKeyBytes(0))
    readReplicationProgresses(Map.empty, iter).foldLeft(Map.empty[String, Long]) {
      case (acc, (nid, progress)) => findId(nid) match {
        case Some(id) => acc + (id -> progress)
        case None     => acc
      }
    }
  }

  @tailrec
  private def readReplicationProgresses(rpMap: Map[Int, Long], iter: RocksIterator): Map[Int, Long] = {
    if (!iter.isValid) rpMap else {
      iter.next()
      val nextKey = rpKey(iter.key())
      if (nextKey == rpKeyEnd) rpMap else {
        val nextVal = RocksdbEventLog.longFromBytes(iter.value())
        readReplicationProgresses(rpMap + (nextKey -> nextVal), iter)
      }
    }
  }

  private def rpKeyBytes(nid: Int): Array[Byte] = {
    val bb = ByteBuffer.allocate(8)
    bb.putInt(classifier)
    bb.putInt(nid)
    bb.array
  }

  private def rpKey(a: Array[Byte]): Int = {
    val bb = ByteBuffer.wrap(a)
    bb.getInt
    bb.getInt
  }
}
