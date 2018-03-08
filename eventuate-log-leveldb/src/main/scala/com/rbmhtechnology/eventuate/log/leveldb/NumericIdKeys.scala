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

import java.nio.ByteBuffer
import java.nio.charset.Charset

/**
 * @author siuming
 */
private[leveldb] trait NumericIdKeys {
  val IdCharset = Charset.forName("UTF-8")
  val IdSequence = "$$SEQUENCE$$"
  val IdSequenceBytes = IdSequence.getBytes(IdCharset)

  def idBytes(classifier: Int, id: String): Array[Byte] = {
    val idBytes = id.getBytes(IdCharset)
    ByteBuffer
      .allocate(idBytes.length + 4)
      .putInt(classifier)
      .put(idBytes)
      .array()
  }

  def intBytes(l: Int): Array[Byte] =
    ByteBuffer.allocate(4).putInt(l).array

  def intFromBytes(a: Array[Byte]): Int =
    ByteBuffer.wrap(a).getInt
}
private[leveldb] object NumericIdKeys extends NumericIdKeys
