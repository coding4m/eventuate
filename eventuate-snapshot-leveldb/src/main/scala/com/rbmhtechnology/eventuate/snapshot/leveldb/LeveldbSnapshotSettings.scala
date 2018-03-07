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

package com.rbmhtechnology.eventuate.snapshot.leveldb

import akka.actor.ActorSystem
import com.typesafe.config.Config

import scala.concurrent.ExecutionContextExecutor

/**
 * @author siuming
 */
class LeveldbSnapshotSettings(system: ActorSystem) {

  implicit val readDispatcher: ExecutionContextExecutor =
    system.dispatchers.lookup("eventuate.snapshot.dispatchers.read-dispatcher")

  implicit val writeDispatcher: ExecutionContextExecutor =
    system.dispatchers.lookup("eventuate.snapshot.dispatchers.write-dispatcher")

  val prefix: String =
    system.settings.config.getString("eventuate.snapshot.leveldb.prefix")

  val rootDir: String =
    system.settings.config.getString("eventuate.snapshot.leveldb.dir")

  val fsync: Boolean =
    system.settings.config.getBoolean("eventuate.snapshot.leveldb.fsync")

  val snapshotsPerMax: Int =
    system.settings.config.getInt("eventuate.snapshot.leveldb.snapshots-per-max")
}
