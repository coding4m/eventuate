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

import akka.actor.{ Actor, PoisonPill, ReceiveTimeout }

import scala.concurrent.duration._

/**
 * @author siuming
 */
trait TerminateOnEvent extends Actor {

  def terminateSettings: TerminateSettings =
    TerminateSettings(PoisonPill, 30.seconds, context.parent)

  @scala.throws[Exception](classOf[Exception])
  override def preStart() = {
    super.preStart()
    context.setReceiveTimeout(terminateSettings.terminateTimeout)
  }

  override def unhandled(message: Any) = message match {
    case ReceiveTimeout => terminateSettings.terminateSupervisor ! terminateSettings.terminateMsg
    case _              => super.unhandled(message)
  }
}
