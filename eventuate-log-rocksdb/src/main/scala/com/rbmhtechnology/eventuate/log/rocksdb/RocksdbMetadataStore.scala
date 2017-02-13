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

import com.rbmhtechnology.eventuate.log.DeletionMetadata
import org.rocksdb.{ ColumnFamilyHandle, RocksDB, WriteOptions }

private class RocksdbMetadataStore(val rocksdb: RocksDB, val rocksdbWriteOptions: WriteOptions, columnHandle: ColumnFamilyHandle) extends RocksdbBatchLayer {
  private val DeletedToSequenceNrKey: Long = 1L
  private val RemoteLogIdsKey: Long = 2L

  def writeDeletionMetadata(info: DeletionMetadata): Unit = withBatch { batch =>
    batch.put(columnHandle, RocksdbEventLog.longBytes(DeletedToSequenceNrKey), RocksdbEventLog.longBytes(info.toSequenceNr))
    batch.put(columnHandle, RocksdbEventLog.longBytes(RemoteLogIdsKey), RocksdbEventLog.stringSetBytes(info.remoteLogIds))
  }

  def readDeletionMetadata(): DeletionMetadata = {
    val toSequenceNr = longFromBytes(rocksdb.get(columnHandle, RocksdbEventLog.longBytes(DeletedToSequenceNrKey)))
    val remoteLogIds = RocksdbEventLog.stringSetFromBytes(rocksdb.get(columnHandle, RocksdbEventLog.longBytes(RemoteLogIdsKey)))
    DeletionMetadata(toSequenceNr, remoteLogIds)
  }

  private def longFromBytes(longBytes: Array[Byte]): Long =
    if (longBytes == null) 0 else RocksdbEventLog.longFromBytes(longBytes)
}
