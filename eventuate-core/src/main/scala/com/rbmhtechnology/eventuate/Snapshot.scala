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

package com.rbmhtechnology.eventuate

import com.rbmhtechnology.eventuate.ConfirmedDelivery.DeliveryAttempt
import com.rbmhtechnology.eventuate.PersistOnEvent.PersistOnEventRequest
import com.typesafe.config.Config

/**
 * Snapshot metadata.
 *
 * @param emitterId  Id of the [[EventsourcedActor]], [[EventsourcedView]], stateful [[EventsourcedWriter]]
 *                   or [[EventsourcedProcessor]] that saves the snapshot.
 * @param sequenceNr The highest event sequence number covered by the snapshot.
 */
case class SnapshotMetadata(emitterId: String, sequenceNr: Long)

/**
 * Provider API.
 *
 * Snapshot storage format. [[EventsourcedActor]]s, [[EventsourcedView]]s, stateful [[EventsourcedWriter]]s
 * and [[EventsourcedProcessor]]s can save snapshots of internal state by calling the (inherited)
 * [[EventsourcedView#save]] method.
 *
 * @param payload                Application-specific snapshot.
 * @param emitterId              Id of the event-sourced actor, view, stateful writer or processor that saved the snapshot.
 * @param lastEvent              Last handled event before the snapshot was saved.
 * @param currentTime            Current vector time when the snapshot was saved.
 * @param sequenceNr             Sequence number of the last *received* event when the snapshot was saved.
 * @param deliveryAttempts       Unconfirmed [[ConfirmedDelivery.DeliveryAttempt DeliveryAttempt]]s when the snapshot was
 *                               saved (can only be non-empty if the actor implements [[ConfirmedDelivery]]).
 * @param persistOnEventRequests Unconfirmed [[PersistOnEvent.PersistOnEventRequest PersistOnEventRequest]]s when the
 *                               snapshot was saved (can only be non-empty if the actor implements [[PersistOnEvent]]).
 */
case class Snapshot(
  payload: Any,
  emitterId: String,
  lastEvent: DurableEvent,
  currentTime: VectorTime,
  sequenceNr: Long,
  deliveryAttempts: Vector[DeliveryAttempt] = Vector.empty,
  persistOnEventRequests: Vector[PersistOnEventRequest] = Vector.empty
) {

  val metadata: SnapshotMetadata =
    SnapshotMetadata(emitterId, sequenceNr)

  def addDeliveryAttempt(deliveryAttempt: DeliveryAttempt): Snapshot =
    copy(deliveryAttempts = deliveryAttempts :+ deliveryAttempt)

  def addPersistOnEventRequest(persistOnEventRequest: PersistOnEventRequest): Snapshot =
    copy(persistOnEventRequests = persistOnEventRequests :+ persistOnEventRequest)
}

class SnapshotSettings(config: Config) {
  val interval = config.getInt("eventuate.snapshot.interval")
}
