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

import org.rocksdb.{ RocksDB, RocksIterator }

import scala.annotation.tailrec

private class RocksdbNumericIdentifierStore(rocksdb: RocksDB, classifier: Int) {
  private var idMap: Map[String, Int] =
    Map.empty

  private val idKeyEnd: Int =
    Int.MaxValue

  private val idKeyEndBytes: Array[Byte] =
    idKeyBytes(idKeyEnd)

  rocksdb.put(idKeyEndBytes, Array.empty[Byte])

  def numericId(id: String): Int = idMap.get(id) match {
    case None    => writeIdMapping(id, idMap.size + 1)
    case Some(v) => v
  }

  def findId(numericId: Int): Option[String] =
    idMap.find { case (_, nid) => nid == numericId }.map(_._1)

  def readIdMap(iter: RocksIterator): Unit = {
    iter.seek(idKeyBytes(0))
    idMap = readIdMap(Map.empty, iter)
  }

  @tailrec
  private def readIdMap(idMap: Map[String, Int], iter: RocksIterator): Map[String, Int] = {
    if (!iter.isValid) idMap else {
      val nextKey = idKey(iter.key())
      if (nextKey == idKeyEnd) idMap else {
        val nextVal = new String(iter.value(), "UTF-8")
        iter.next()
        readIdMap(idMap + (nextVal -> nextKey), iter)
      }
    }
  }

  private def writeIdMapping(id: String, nid: Int): Int = {
    idMap = idMap + (id -> nid)
    rocksdb.put(idKeyBytes(nid), id.getBytes("UTF-8"))
    nid
  }

  private def idKeyBytes(nid: Int): Array[Byte] = {
    // key1 is at first when key1.length > key2.length
    // to ensure key order, key length same to event key length.
    val bb = ByteBuffer.allocate(12)
    bb.putInt(classifier)
    bb.putLong(nid.toLong)
    bb.array
  }

  private def idKey(a: Array[Byte]): Int = {
    val bb = ByteBuffer.wrap(a)
    bb.getInt
    bb.getLong.toInt
  }
}
