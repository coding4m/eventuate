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

import akka.actor.{ Actor, ReceiveTimeout }

import scala.concurrent.duration._

/**
 * @author siuming
 */
trait ShutdownOnTimeout extends Actor {

  def deadline: ShutdownDeadline =
    ShutdownDeadline(context.parent, Shutdown, 30.seconds)

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    context.setReceiveTimeout(deadline.timeout)
  }

  override def unhandled(message: Any): Unit = message match {
    case ReceiveTimeout => deadline.target ! deadline.message
    case _              => super.unhandled(message)
  }

  protected final def operationQueued(): Unit = {
    context.setReceiveTimeout(Duration.Inf)
  }

  protected final def operationCompleted(): Unit = {
    context.setReceiveTimeout(deadline.timeout)
  }

  protected final def suspend(f: => Unit): Unit = f

  protected final def suspend(): Unit = context.setReceiveTimeout(deadline.timeout)

  protected final def shutdown(f: => Unit): Unit = {
    try {
      f
    } finally {
      shutdown()
    }
  }

  protected final def shutdown(): Unit = deadline.target ! deadline.message
}
case object Shutdown
