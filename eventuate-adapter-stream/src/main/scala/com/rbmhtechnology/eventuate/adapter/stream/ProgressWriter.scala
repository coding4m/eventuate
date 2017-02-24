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

package com.rbmhtechnology.eventuate.adapter.stream

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.{ ActorRef, ActorSystem }
import akka.pattern._
import akka.stream._
import akka.stream.scaladsl.Flow
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import akka.util.Timeout
import com.rbmhtechnology.eventuate.ReplicationProtocol.{ SetReplicationProgress, SetReplicationProgressFailure, SetReplicationProgressSuccess }
import com.typesafe.config.Config

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

/**
 * @author siuming
 */
private class ProgressWriterSettings(config: Config) {
  val writeTimeout =
    config.getDuration("eventuate.log.write-timeout", TimeUnit.MILLISECONDS).millis
}
object ProgressWriter {

  /**
   * Creates a graph stage that write the processing progress for `sourceLogId` from `targetLog`.
   * Behavior of the progress writer can be configured with:
   *  - `eventuate.log.write-timeout`. Timeout for writting a processing progress value to the event log. A write timeout
   *  or another write failure causes this stage to fail.
   */
  def apply(sourceLogId: String, targetLog: ActorRef)(implicit system: ActorSystem): Graph[FlowShape[Long, Long], NotUsed] = {
    implicit val timeout = Timeout(new ProgressWriterSettings(system.settings.config).writeTimeout)

    Flow.fromGraph[Long, Long, NotUsed](new GraphStage[FlowShape[Long, Long]] {
      val in = Inlet[Long]("ProgressWriter.in")
      val out = Outlet[Long]("ProgressWriter.out")

      override def shape = FlowShape.of(in, out)

      override def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) {

        var writing = false
        var finished = false

        setHandler(in, new InHandler {
          override def onPush() = {
            val sequenceNr = grab(in)
            val callback = getAsyncCallback[Try[Long]] {
              case Success(r) =>
                writing = false
                push(out, r)
                if (finished) {
                  completeStage()
                }
              case Failure(t) =>
                failStage(t)
            }

            targetLog.ask(SetReplicationProgress(sourceLogId, sequenceNr)).flatMap({
              case SetReplicationProgressSuccess(_, progress) => Future.successful(progress)
              case SetReplicationProgressFailure(cause)       => Future.failed(cause)
            })(materializer.executionContext).onComplete(callback.invoke)(materializer.executionContext)
            writing = true
          }

          override def onUpstreamFinish() = if (writing) {
            // defer stage completion
            finished = true
          } else super.onUpstreamFinish()
        })

        setHandler(out, new OutHandler {
          override def onPull() = pull(in)
        })
      }
    })
  }
}
