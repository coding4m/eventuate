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

package com.rbmhtechnology.eventuate.log.leveldb

import org.iq80.leveldb.{ DB, WriteOptions }

private[leveldb] class NumericIdStore(val leveldb: DB, val writeOptions: WriteOptions, classifier: Long) extends LeveldbBatchLayer {
  import NumericIdKeys._

  if (null == leveldb.get(IdSequenceBytes)) {
    leveldb.put(IdSequenceBytes, longBytes(1L))
  }

  def numericId(stringId: String, readOnly: Boolean = false): Long = {
    assert(stringId != IdSequence, s"id must not eq $IdSequence .")
    val nid = leveldb.get(idBytes(classifier, stringId))
    if (null == nid) {
      if (readOnly) Long.MaxValue else writeNumericId(stringId)
    } else longFromBytes(nid)
  }

  private def writeNumericId(stringId: String) = withBatch { batch =>
    val nidBytes = leveldb.get(IdSequenceBytes)
    val nid = longFromBytes(nidBytes)
    batch.put(idBytes(classifier, stringId), nidBytes)
    batch.put(IdSequenceBytes, longBytes(nid + 1L))
    leveldb.write(batch, writeOptions)
    nid
  }
}
