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

import akka.actor.{ Actor, ActorRef, Props }

/**
 * @author siuming
 */
trait EventsourcedPool[ID] { this: Actor =>

  var aggregateRefs: Map[ID, ActorRef] = Map.empty[ID, ActorRef]

  def aggregateProjection: ID => Props
  def aggregateIdProjection: PartialFunction[Any, ID]

  /**
   *
   * @param actorRef
   */
  protected def resize(actorRef: ActorRef): Unit = aggregateRefs.filter(value => value._2 == actorRef).foreach { value =>
    aggregateRefs = aggregateRefs - value._1
    context stop value._2
  }

  final protected def <~>(actorRef: ActorRef): Unit =
    resize(actorRef)

  /**
   *
   * @param message
   * @param sender
   */
  protected def relay(message: Any, sender: ActorRef): Unit = {
    val aggregateId = aggregateIdProjection(message)
    aggregateOf(aggregateId).tell(message, sender)
  }

  final protected def ~~>(message: Any, sender: ActorRef): Unit =
    relay(message, sender)

  private def aggregateOf(aggregateId: ID): ActorRef = aggregateRefs.get(aggregateId) match {
    case Some(actorRef) => actorRef
    case None =>
      val actorRef = context.actorOf(aggregateProjection(aggregateId))
      aggregateRefs = aggregateRefs + (aggregateId -> actorRef)
      aggregateRefs(aggregateId)
  }
}
