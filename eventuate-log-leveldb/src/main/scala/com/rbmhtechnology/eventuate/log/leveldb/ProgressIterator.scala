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

import java.io.Closeable
import java.nio.ByteBuffer

import org.iq80.leveldb.DBIterator

/**
 * @author siuming
 */
private[leveldb] object ProgressIterator {
  def apply(it: DBIterator, classifier: Long): ProgressIterator = new ProgressIterator(it, classifier)
}
private[leveldb] class ProgressIterator(it: DBIterator, classifier: Long) extends Iterator[ProgressItem] with Closeable {
  import ProgressKeys._
  val iter1 = new Iterator[(Long, Array[Byte], Array[Byte])] {
    override def hasNext = it.hasNext
    override def next() = {
      val entry = it.next()
      (ByteBuffer.wrap(entry.getKey.slice(0, 8)).getLong, entry.getKey, entry.getValue)
    }
  }.takeWhile(_._1 == classifier)
  val iter2 = new Iterator[ProgressItem] {
    override def hasNext = iter1.hasNext
    override def next() = {
      val entry = iter1.next()
      val key = progressKey(entry._2)
      val value = longFromBytes(entry._3)
      ProgressItem(entry._1, key.id, value)
    }
  }

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
  override def hasNext = iter2.hasNext
  override def next() = iter2.next()
  override def close() = it.close()
}
