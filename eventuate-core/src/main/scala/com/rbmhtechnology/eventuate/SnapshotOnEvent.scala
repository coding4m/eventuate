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

import akka.actor.MessagePipeline
import akka.actor.MessagePipeline.Inner

/**
 * @author siuming
 */
trait SnapshotOnEvent { this: EventsourcedView with MessagePipeline =>

  import EventsourcingProtocol._

  private var snapshotOffset: Int = 0
  private var snapshotInProgress: Boolean = false
  private val snapshotSettings = new SnapshotSettings(context.system.settings.config)

  private def snapshotOrDelay() = {
    val extraOffset = if (snapshotInProgress) snapshotSettings.interval else 0
    if (snapshotOffset - extraOffset >= snapshotSettings.interval) {
      self ! Snapshotting
      snapshotInProgress = true
    }
  }

  pipelineOuter {

    case ss: SaveSnapshotSuccess =>
      snapshotOffset = 0
      snapshotInProgress = false
      Inner(ss)

    case ss: SaveSnapshotFailure =>
      snapshotOffset = 0
      snapshotInProgress = false
      Inner(ss)

    case rs @ ReplaySuccess(events, _, _) =>
      snapshotOffset += events.size
      snapshotOrDelay()
      Inner(rs)

    case ws @ WriteSuccess(events, _, _) =>
      Inner(ws) andAfter {
        snapshotOffset += events.size
        snapshotOrDelay()
      }

    // sent by an event log when replicate success
    case w: Written =>
      snapshotOffset += 1
      snapshotOrDelay()
      Inner(w)

    case any =>
      Inner(any)
  }

}

