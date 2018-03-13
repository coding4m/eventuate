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

package com.rbmhtechnology.eventuate.snapshot.cassandra

import com.typesafe.config.Config

/**
 * @author siuming
 */
class CassandraSnapshotSettings(config: Config) {
  val keyspace: String =
    config.getString("eventuate.snapshot.cassandra.keyspace")

  val keyspaceAutoCreate: Boolean =
    config.getBoolean("eventuate.snapshot.cassandra.keyspace-autocreate")

  val replicationFactor: Int =
    config.getInt("eventuate.snapshot.cassandra.replication-factor")

  val tablePrefix: String =
    config.getString("eventuate.snapshot.cassandra.table-prefix")

}