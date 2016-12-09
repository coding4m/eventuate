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

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ Actor, ActorRef, MessageStash, Stash }
import akka.event.{ Logging, LoggingAdapter }
import com.rbmhtechnology.eventuate.EventsourcingProtocol.{ Write, WriteFailure, WriteSuccess }

import scala.util.{ Failure, Success, Try }

object EventsourcedPersister {
  type Handler[A] = (ActorRef, Try[A]) => Unit

  object Handler {
    def empty[A]: Handler[A] = (_: ActorRef, _: Try[A]) => Unit
  }

  /**
   * Internal API.
   */
  private[eventuate] val instanceIdCounter = new AtomicInteger(0)
}

trait EventsourcedPersister extends Actor with Stash {

  import EventsourcedPersister._

  private val instanceId =
    instanceIdCounter.incrementAndGet()

  private val messageStash = new MessageStash()
  private val commandStash = new MessageStash()

  private var writeRequests: Vector[DurableEvent] = Vector.empty
  private var writeHandlers: Vector[Handler[Any]] = Vector.empty

  private var writeRequestCorrelationId: Int = 0
  private var writesInProgress: Set[Int] = Set.empty

  private var writing: Boolean = false
  private var writeReplyHandling: Boolean = false

  private lazy val _commandContext: BehaviorContext = new DefaultBehaviorContext(onCommand)

  /**
   * This actor's logging adapter.
   */
  val logger: LoggingAdapter =
    Logging(context.system, this)

  def id: Option[String] = None

  /**
   * Event log actor.
   */
  def eventLog: ActorRef

  /**
   * Returns the command [[BehaviorContext]].
   */
  def commandContext: BehaviorContext =
    _commandContext

  /**
   * State synchronization. If set to `true`, commands see internal state that is consistent
   * with the event log. This is achieved by stashing new commands if this actor is currently
   * writing events. If set to `false`, commands see internal state that is eventually
   * consistent with the event log.
   */
  //#state-sync
  def stateSync: Boolean = true

  /**
   * Asynchronously persists a sequence of `events` and calls `handler` with the persist result
   * for each event in the sequence. If persistence was successful, `onEvent` is called with a
   * persisted event before `handler` is called. Both, `onEvent` and `handler`, are called on a
   * dispatcher thread of this actor, hence, it is safe to modify internal state within them.
   * The `handler` can also obtain a reference to the initial command sender via `sender()`. The
   * `onLast` handler is additionally called for the last event in the sequence.
   *
   * By default, the event is routed to event-sourced destinations with an undefined `aggregateId`.
   * If this actor's `aggregateId` is defined it is additionally routed to all actors with the same
   * `aggregateId`. Further routing destinations can be defined with the `customDestinationAggregateIds`
   * parameter.
   */
  final def persistN[A](events: Seq[A], onLast: Handler[A] = Handler.empty[A], customDestinationAggregateIds: Set[String] = Set())(handler: Handler[A]): Unit = events match {
    case Seq() =>
    case es :+ e =>
      es.foreach { event =>
        persist(event, customDestinationAggregateIds)(handler)
      }
      persist(e, customDestinationAggregateIds) { (sdr, r) =>
        handler(sdr, r)
        onLast(sdr, r)
      }
  }

  /**
   * Asynchronously persists the given `event` and calls `handler` with the persist result. If
   * persistence was successful, `onEvent` is called with the persisted event before `handler`
   * is called. Both, `onEvent` and `handler`, are called on a dispatcher thread of this actor,
   * hence, it is safe to modify internal state within them. The `handler` can also obtain a
   * reference to the initial command sender via `sender()`.
   *
   * By default, the event is routed to event-sourced destinations with an undefined `aggregateId`.
   * If this actor's `aggregateId` is defined it is additionally routed to all actors with the same
   * `aggregateId`. Further routing destinations can be defined with the `customDestinationAggregateIds`
   * parameter.
   */
  final def persist[A](event: A, customDestinationAggregateIds: Set[String] = Set())(handler: Handler[A]): Unit =
    persistDurableEvent(durableEvent(event, customDestinationAggregateIds), handler.asInstanceOf[Handler[Any]])

  /**
   * Internal API.
   */
  private[eventuate] def persistDurableEvent(event: DurableEvent, handler: Handler[Any]): Unit = {
    writeRequests = writeRequests :+ event
    writeHandlers = writeHandlers :+ handler
  }

  /**
   * Command handler.
   */
  def onCommand: Receive

  final def receive: Receive = {
    case WriteSuccess(events, cid, iid) => if (writesInProgress.contains(cid) && iid == instanceId) writeReplyHandling(cid) {
      val sdr = sender()
      events.foreach { event =>
        writeHandlers.head(sdr, Success(event.payload))
        writeHandlers = writeHandlers.tail
      }
      if (stateSync) {
        writing = false
        messageStash.unstash()
      }
    }
    case WriteFailure(events, cause, cid, iid) => if (writesInProgress.contains(cid) && iid == instanceId) writeReplyHandling(cid) {
      val sdr = sender()
      events.foreach { _ =>
        writeHandlers.head(sdr, Failure(cause))
        writeHandlers = writeHandlers.tail
      }
      if (stateSync) {
        writing = false
        messageStash.unstash()
      }
    }
    case msg =>
      writeOrDelay(unhandledMessage(msg))
  }

  /**
   * Internal API.
   */
  private[eventuate] def unhandledMessage(msg: Any): Unit = {
    val behavior = _commandContext.current
    if (behavior.isDefinedAt(msg)) behavior(msg) else unhandled(msg)
  }

  private def writeReplyHandling(correlationId: Int)(body: => Unit): Unit =
    try {
      writeReplyHandling = true
      body
    } finally {
      writeReplyHandling = false
      writesInProgress = writesInProgress - correlationId
    }

  private def writePending: Boolean =
    writeRequests.nonEmpty

  private def writeOrDelay(writeProducer: => Unit): Unit = {
    if (writing) messageStash.stash() else {
      writeProducer

      val wPending = writePending
      if (wPending) write(nextCorrelationId())
      if (wPending && stateSync) writing = true else if (stateSync) messageStash.unstash()
    }
  }

  private def write(correlationId: Int): Unit = {
    eventLog ! Write(writeRequests, sender(), self, correlationId, instanceId)
    writesInProgress = writesInProgress + correlationId
    writeRequests = Vector.empty
  }

  private def nextCorrelationId(): Int = {
    writeRequestCorrelationId += 1
    writeRequestCorrelationId
  }

  /**
   * Internal API.
   */
  protected def durableEvent(payload: Any, customDestinationAggregateIds: Set[String], deliveryId: Option[String] = None): DurableEvent =
    DurableEvent(
      payload = payload,
      emitterId = id.getOrElse(DurableEvent.UndefinedEmittedId),
      customDestinationAggregateIds = customDestinationAggregateIds,
      deliveryId = deliveryId)

  /**
   * Adds the current command to the user's command stash. Must not be used in the event handler
   * or `persist` handler.
   */
  override def stash(): Unit =
    if (writeReplyHandling) throw new StashError("stash() must not be used persist handler") else commandStash.stash()

  /**
   * Prepends all stashed commands to the actor's mailbox and then clears the command stash.
   */
  override def unstashAll(): Unit = {
    commandStash ++: messageStash
    commandStash.clear()
    messageStash.unstashAll()
  }
}
