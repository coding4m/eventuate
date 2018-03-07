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

package com.rbmhtechnology.eventuate.snapshot

import akka.actor.ActorSystem
import com.rbmhtechnology.eventuate.Snapshot

import scala.concurrent.Future

/**
 * Snapshot store provider interface.
 */
trait SnapshotStoreSPI {

  /**
   * Asynchronously loads the latest snapshot saved by an event-sourced actor, view, writer or processor
   * identified by `emitterId`.
   */
  def load(emitterId: String): Future[Option[Snapshot]]

  /**
   * Asynchronously saves the given `snapshot`.
   */
  def save(snapshot: Snapshot): Future[Unit]

  /**
   * Asynchronously deletes all snapshots with `emitterId`.
   */
  def delete(emitterId: String): Future[Unit]

  /**
   * Asynchronously deletes all snapshots with a sequence number greater than or equal `lowerSequenceNr`.
   */
  def delete(lowerSequenceNr: Long): Future[Unit]
}
abstract class SnapshotStore(actorSystem: ActorSystem, id: String) extends SnapshotStoreSPI
