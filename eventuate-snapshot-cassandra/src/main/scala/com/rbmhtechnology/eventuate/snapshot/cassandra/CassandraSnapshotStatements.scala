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

/**
 * @author siuming
 */
trait CassandraSnapshotStatements {
  val settings: CassandraSnapshotSettings

  def createKeySpaceStatement = s"""
      CREATE KEYSPACE IF NOT EXISTS ${settings.keyspace}
      WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : ${settings.replicationFactor} }
    """

  def createSnapshotTableStatement(logId: String) = s"""
      CREATE TABLE IF NOT EXISTS ${settings.keyspace}.${settings.tablePrefix}.$logId (
        aggregate_id text,
        sequence_nr bigint,
        snapshot blob,
        PRIMARY KEY (aggregate_id, sequence_nr)
      )
    """
}