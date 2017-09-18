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

package com.rbmhtechnology.eventuate.log

import akka.actor.{ ActorSystem, Props }
import com.rbmhtechnology.eventuate.log.cassandra.CassandraEventLog
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog
import com.rbmhtechnology.eventuate.log.rocksdb.RocksdbEventLog

/**
 * @author siuming
 */
object EventLogAdapter {

  val LevelDB = "leveldb"
  val RocksDB = "rocksdb"
  val Cassandra = "cassandra"

  def apply(logId: String, batching: Boolean = true)(implicit system: ActorSystem): Props = {
    val settings = new EventLogAdapterSettings(system)
    if (settings.adapterName == LevelDB) {
      LeveldbEventLog.props(logId, batching = batching)
    } else if (settings.adapterName == RocksDB) {
      RocksdbEventLog.props(logId, batching = batching)
    } else if (settings.adapterName == Cassandra) {
      CassandraEventLog.props(logId, batching = batching)
    } else throw new EventLogUnsupportedException
  }
}
