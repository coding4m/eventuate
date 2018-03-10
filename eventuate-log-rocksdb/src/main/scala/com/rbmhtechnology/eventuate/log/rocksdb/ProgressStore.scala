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

import org.rocksdb._

import scala.annotation.tailrec

private class ProgressStore(val rocksdb: RocksDB, val writeOptions: WriteOptions, columnHandle: ColumnFamilyHandle) extends RocksdbBatchLayer {
  import EventKeys._

  def writeProgresses(progresses: Map[String, Long]): Unit = withBatch { batch =>
    progresses.foreach(it => batch.put(columnHandle, stringBytes(it._1), longBytes(it._2)))
  }

  def readProgress(logId: String): Long = {
    val progress = rocksdb.get(columnHandle, stringBytes(logId))
    if (progress == null) 0L else longFromBytes(progress)
  }

  def readProgresses(): Map[String, Long] = {
    val options = new ReadOptions().setVerifyChecksums(false).setSnapshot(rocksdb.getSnapshot)
    val iter = rocksdb.newIterator(columnHandle, options)
    try {
      iter.seekToFirst()
      readProgresses(Map.empty[String, Long], iter)
    } finally {
      iter.close()
      options.snapshot().close()
    }

  }

  @tailrec
  private def readProgresses(progresses: Map[String, Long], iter: RocksIterator): Map[String, Long] = {
    if (!iter.isValid) progresses else {
      val key = stringFromBytes(iter.key())
      val value = longFromBytes(iter.value())
      iter.next()
      readProgresses(progresses + (key -> value), iter)
    }
  }
}
