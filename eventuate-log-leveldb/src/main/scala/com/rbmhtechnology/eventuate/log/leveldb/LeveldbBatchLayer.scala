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

import org.iq80.leveldb.{ DB, WriteBatch, WriteOptions }

/**
 * @author siuming
 */
trait LeveldbBatchLayer {
  protected def leveldb: DB
  protected def writeOptions: WriteOptions

  protected def withBatch[R](body: WriteBatch => R): R = {
    val batch = leveldb.createWriteBatch()
    try {
      val r = body(batch)
      leveldb.write(batch, writeOptions)
      r
    } finally {
      batch.close()
    }
  }
}
