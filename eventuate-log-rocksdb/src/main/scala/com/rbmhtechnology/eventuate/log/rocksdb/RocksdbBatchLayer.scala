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

import org.rocksdb.{ RocksDB, WriteBatch, WriteOptions }

/**
 * @author siuming
 */
private[rocksdb] trait RocksdbBatchLayer {
  protected def rocksdb: RocksDB
  protected def rocksdbWriteOptions: WriteOptions

  protected def withBatch[R](body: WriteBatch => R): R = {
    val batch = new WriteBatch()
    try {
      val r = body(batch)
      rocksdb.write(rocksdbWriteOptions, batch)
      r
    } finally {
      batch.close()
    }
  }
}