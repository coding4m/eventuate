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

import akka.actor.ActorSystem
import com.rbmhtechnology.eventuate.Snapshot
import com.rbmhtechnology.eventuate.snapshot.SnapshotStore

/**
 * @author siuming
 */
class CassandraSnapshotStore(system: ActorSystem, id: String) extends SnapshotStore(system, id) {
  /**
   * Asynchronously loads the latest snapshot saved by an event-sourced actor, view, writer or processor
   * identified by `emitterId`.
   */
  override def load(emitterId: String) = ???

  /**
   * Asynchronously saves the given `snapshot`.
   */
  override def save(snapshot: Snapshot) = ???

  /**
   * Asynchronously deletes all snapshots with `emitterId`.
   */
  override def delete(emitterId: String) = ???

  /**
   * Asynchronously deletes all snapshots with a sequence number greater than or equal `lowerSequenceNr`.
   */
  override def delete(lowerSequenceNr: Long) = ???

  override def close() = ???
}
