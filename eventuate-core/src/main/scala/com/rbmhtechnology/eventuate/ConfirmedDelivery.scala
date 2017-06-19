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

import akka.actor._

import scala.collection.immutable.SortedMap

object ConfirmedDelivery {
  case class DeliveryAttempt(deliveryId: String, message: Any, destination: ActorPath)
}

/**
 * Supports the reliable delivery of messages to destinations by enabling applications to
 * redeliver messages until they are confirmed by their destinations. The correlation
 * identifier between a reliable message and its confirmation message is an
 * application-defined `deliveryId`. Reliable messages are delivered by calling `deliver` in
 * an [[EventsourcedActor]]'s event handler. When the destination replies with a confirmation
 * message, the event-sourced actor must persist an application-defined confirmation event
 * together with the `deliveryId` using the [[persistConfirmation]] method. Until successful
 * persistence of the confirmation event, delivered messages are tracked as ''unconfirmed''
 * messages. Unconfirmed messages can be redelivered by calling `redeliverUnconfirmed`. This
 * is usually done within a command handler by processing scheduler messages. Redelivery
 * occurs automatically when the event-sourced actor successfully recovered after initial
 * start or a re-start.
 */
trait ConfirmedDelivery extends EventsourcedActor {
  import ConfirmedDelivery._

  private var _unconfirmed: SortedMap[String, DeliveryAttempt] = SortedMap.empty

  /**
   * Same semantics as [[EventsourcedActor.persist]] plus additional storage of a `deliveryId`
   * together with the persistent `event`.
   */
  final def persistConfirmation[A](event: A, deliveryId: String, customDestinationAggregateIds: Set[String] = Set())(handler: Handler[A]): Unit =
    persistDurableEvent(durableEvent(event, customDestinationAggregateIds, Some(deliveryId)), handler.asInstanceOf[Handler[Any]])

  /**
   * Delivers the given `message` to a `destination`. The delivery of `message` is identified by
   * the given `deliveryId` which must be unique in context of the sending actor. The message is
   * tracked as unconfirmed message until delivery is confirmed by persisting a confirmation event
   * with `persistConfirmation`, using the same `deliveryId`.
   */
  final def deliver(deliveryId: String, message: Any, destination: ActorPath): Unit = {
    _unconfirmed = _unconfirmed + (deliveryId -> DeliveryAttempt(deliveryId, message, destination))
    if (!recovering) deliverInternal(deliveryId, message, destination)
  }

  protected def deliverInternal(deliveryId: String, message: Any, destination: ActorPath): Unit =
    context.actorSelection(destination) ! message

  /**
   * Redelivers all unconfirmed messages.
   */
  final def redeliverUnconfirmed(): Unit = _unconfirmed.foreach {
    case (_, DeliveryAttempt(i, m, d)) => deliverInternal(i, m, d)
  }

  /**
   * Redelivers unconfirmed message with deliveryId.
   */
  final def redeliverUnconfirmed(deliveryId: String): Unit = _unconfirmed.get(deliveryId).foreach { attempt =>
    deliverInternal(attempt.deliveryId, attempt.message, attempt.destination)
  }

  /**
   * Delivery ids of unconfirmed messages.
   */
  final def unconfirmed: Set[String] =
    _unconfirmed.keySet

  /**
   * Internal API.
   */
  override private[eventuate] def receiveEvent(event: DurableEvent): Unit = {
    super.receiveEvent(event)

    event.deliveryId.foreach { deliveryId =>
      if (event.emitterId == id) _unconfirmed = _unconfirmed - deliveryId
    }
  }

  /**
   * Internal API.
   */
  override private[eventuate] def snapshotCaptured(snapshot: Snapshot): Snapshot = {
    _unconfirmed.values.foldLeft(super.snapshotCaptured(snapshot)) {
      case (s, da) => s.addDeliveryAttempt(da)
    }
  }

  /**
   * Internal API.
   */
  override private[eventuate] def snapshotLoaded(snapshot: Snapshot): Unit = {
    super.snapshotLoaded(snapshot)
    snapshot.deliveryAttempts.foreach { da =>
      _unconfirmed = _unconfirmed + (da.deliveryId -> da)
    }
  }

  /**
   * Internal API.
   */
  private[eventuate] override def recovered(): Unit = {
    super.recovered()
    redeliverUnconfirmed()
  }
}
