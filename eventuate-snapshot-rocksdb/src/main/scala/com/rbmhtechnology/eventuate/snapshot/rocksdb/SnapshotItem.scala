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

import java.nio.ByteBuffer
import java.nio.charset.Charset

import akka.serialization.Serialization
import com.rbmhtechnology.eventuate.Snapshot

/**
 * @author siuming
 */
private[rocksdb] object SnapshotItem {
  val StringCharset = Charset.forName("UTF-8")
  def item(key: Array[Byte], value: Array[Byte])(implicit serialization: Serialization) = {
    SnapshotItem(
      key,
      new String(key.slice(0, key.length - 8), StringCharset),
      ByteBuffer.wrap(key, key.length - 8, key.length).getLong,
      serialization.deserialize(value, classOf[Snapshot]).get
    )
  }
  def itemKey(emitterId: String, sequenceNr: Long): Array[Byte] = {
    val stringBytes = emitterId.getBytes(StringCharset)
    ByteBuffer.allocate(stringBytes.length + 8).put(stringBytes).putLong(sequenceNr).array()
  }

  def itemValue(snapshot: Snapshot)(implicit serialization: Serialization): Array[Byte] = {
    serialization.serialize(snapshot).get
  }

  def firstKey(emitterId: String): Array[Byte] = itemKey(emitterId, 0L)
  def lastKey(emitterId: String): Array[Byte] = itemKey(emitterId, Long.MaxValue)
}
private[rocksdb] case class SnapshotItem(key: Array[Byte], emitterId: String, sequenceNr: Long, snapshot: Snapshot)
