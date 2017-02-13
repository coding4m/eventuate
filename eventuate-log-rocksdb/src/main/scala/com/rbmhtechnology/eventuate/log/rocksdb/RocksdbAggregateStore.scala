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

import org.rocksdb.{ ColumnFamilyHandle, RocksDB }

private class RocksdbAggregateStore(rocksdb: RocksDB, columnHandle: ColumnFamilyHandle) {

  private val IdSequence = "$SEQUENCE$"
  private val IdSequenceBytes = RocksdbEventLog.stringBytes(IdSequence)

  private val IdSequenceInc = 1
  private val IdSequenceIncBytes = RocksdbEventLog.longBytes(IdSequenceInc)

  if (null == rocksdb.get(columnHandle, IdSequenceBytes)) {
    rocksdb.put(columnHandle, IdSequenceBytes, RocksdbEventLog.longBytes(1L))
  }

  def numericId(aggregateId: String): Int = {
    assert(aggregateId != IdSequence, s"aggregateId must not eq $IdSequence .")
    val res = rocksdb.get(columnHandle, RocksdbEventLog.stringBytes(aggregateId))
    if (null == res) writeNumericId(aggregateId) else RocksdbEventLog.longFromBytes(res).toInt
  }

  private def writeNumericId(aggregateId: String) = {
    // todo use transaction api.
    val nidBytes = rocksdb.get(columnHandle, IdSequenceBytes)
    val nid = RocksdbEventLog.longFromBytes(nidBytes)
    rocksdb.put(columnHandle, RocksdbEventLog.stringBytes(aggregateId), nidBytes)
    rocksdb.merge(columnHandle, IdSequenceBytes, IdSequenceIncBytes)
    nid.toInt
  }
}
