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

import org.rocksdb.{ ColumnFamilyHandle, RocksDB, WriteOptions }

private class NumericIdStore(val rocksdb: RocksDB, val writeOptions: WriteOptions, columnHandle: ColumnFamilyHandle) extends RocksdbBatchLayer {
  import EventKeys._
  import NumericIdKeys._

  if (null == rocksdb.get(columnHandle, IdSequenceBytes)) {
    rocksdb.put(columnHandle, IdSequenceBytes, longBytes(1L))
  }

  def numericId(stringId: String, readOnly: Boolean = false): Long = {
    assert(stringId != IdSequence, s"id must not eq $IdSequence .")
    val nid = rocksdb.get(columnHandle, stringBytes(stringId))
    if (null != nid) longFromBytes(nid)
    else if (readOnly) Long.MaxValue
    else writeNumericId(stringId)
  }

  private def writeNumericId(stringId: String) = withBatch { batch =>
    val nidBytes = rocksdb.get(columnHandle, IdSequenceBytes)
    val nid = longFromBytes(nidBytes)
    batch.put(columnHandle, stringBytes(stringId), nidBytes)
    batch.merge(columnHandle, IdSequenceBytes, IdSequenceIncBytes)
    nid
  }
}
