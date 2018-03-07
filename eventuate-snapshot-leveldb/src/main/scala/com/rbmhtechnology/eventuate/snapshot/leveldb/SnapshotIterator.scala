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

package com.rbmhtechnology.eventuate.snapshot.leveldb

import java.io.Closeable

import akka.serialization.Serialization
import org.iq80.leveldb.DBIterator

/**
 * @author siuming
 */
private[leveldb] object SnapshotIterator {
  def apply(it: DBIterator, reserved: Boolean = true)(implicit serialization: Serialization): SnapshotIterator = new SnapshotIterator(it, reserved)
}
private[leveldb] class SnapshotIterator(it: DBIterator, reserved: Boolean)(implicit serialization: Serialization) extends Iterator[SnapshotItem] with Closeable {
  def first(emitterId: String): SnapshotIterator = seek(SnapshotItem.firstKey(emitterId))
  def last(emitterId: String): SnapshotIterator = seek(SnapshotItem.lastKey(emitterId))
  def seek(key: Array[Byte]) = {
    it.seek(key)
    this
  }
  def seekToFirst() = {
    it.seekToFirst()
    this
  }
  def seekToLast() = {
    it.seekToLast()
    this
  }
  override def hasNext = if (reserved) it.hasPrev else it.hasNext
  override def next() = {
    val item = if (reserved) it.prev() else it.next()
    SnapshotItem.item(item.getKey, item.getValue)
  }
  override def close() = it.close()
}
