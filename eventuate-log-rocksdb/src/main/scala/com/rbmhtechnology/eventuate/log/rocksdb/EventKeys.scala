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
import java.nio.charset.Charset

/**
 * @author siuming
 */
private[rocksdb] trait EventKeys {
  val DefaultClassifier: Long = 0L
  val StringCharset = Charset.forName("UTF-8")
  val StringSetSeparator = '\u0000'

  def longBytes(l: Long): Array[Byte] =
    ByteBuffer.allocate(8).putLong(l).array

  def longFromBytes(a: Array[Byte]): Long =
    ByteBuffer.wrap(a).getLong

  def stringBytes(s: String): Array[Byte] =
    s.getBytes(StringCharset)

  def stringFromBytes(a: Array[Byte]) =
    new String(a, StringCharset)

  def stringSetBytes(set: Set[String]): Array[Byte] =
    stringBytes(set.mkString(StringSetSeparator.toString))

  def stringSetFromBytes(setBytes: Array[Byte]): Set[String] = {
    if (setBytes == null || setBytes.length == 0) Set.empty
    else stringFromBytes(setBytes).split(StringSetSeparator).toSet
  }

  val eventKeyEnd: EventKey =
    EventKey(Long.MaxValue, Long.MaxValue)

  val eventKeyEndBytes: Array[Byte] =
    eventKeyBytes(eventKeyEnd.classifier, eventKeyEnd.sequenceNr)

  def eventKeyBytes(classifier: Long, sequenceNr: Long): Array[Byte] = {
    val bb = ByteBuffer.allocate(16)
    bb.putLong(classifier)
    bb.putLong(sequenceNr)
    bb.array
  }

  def eventKeyFromBytes(a: Array[Byte]): EventKey = {
    val bb = ByteBuffer.wrap(a)
    EventKey(bb.getLong, bb.getLong)
  }
}
private[rocksdb] object EventKeys extends EventKeys
