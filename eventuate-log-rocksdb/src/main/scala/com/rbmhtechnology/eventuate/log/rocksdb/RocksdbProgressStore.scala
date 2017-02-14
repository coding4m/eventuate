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

import org.rocksdb.{ ColumnFamilyHandle, RocksDB, RocksIterator, WriteBatch }

import scala.annotation.tailrec

private class RocksdbProgressStore(rocksdb: RocksDB, columnHandle: ColumnFamilyHandle) {
  import RocksdbEventLog._

  def writeReplicationProgress(logId: String, logSnr: Long, batch: WriteBatch): Unit = {
    batch.put(columnHandle, stringBytes(logId), longBytes(logSnr))
  }

  def readReplicationProgress(logId: String): Long = {
    val progress = rocksdb.get(columnHandle, stringBytes(logId))
    if (progress == null) 0L else longFromBytes(progress)
  }

  def readReplicationProgresses(iter: RocksIterator): Map[String, Long] = {
    iter.seekToFirst()
    readReplicationProgresses(Map.empty[String, Long], iter)
  }

  @tailrec
  private def readReplicationProgresses(rpMap: Map[String, Long], iter: RocksIterator): Map[String, Long] = {
    if (!iter.isValid) rpMap else {
      val nextKey = stringFromBytes(iter.key())
      val nextVal = longFromBytes(iter.value())
      iter.next()
      readReplicationProgresses(rpMap + (nextKey -> nextVal), iter)
    }
  }
}
