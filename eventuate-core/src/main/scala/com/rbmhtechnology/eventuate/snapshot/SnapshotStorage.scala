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

import akka.actor.{ ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider }

/**
 * @author siuming
 */
object SnapshotStorage extends ExtensionId[SnapshotStorage] with ExtensionIdProvider {
  override def lookup() = SnapshotStorage
  override def createExtension(system: ExtendedActorSystem) = new SnapshotStorage(system)
}
class SnapshotStorage(system: ExtendedActorSystem) extends Extension {

  // todo
  private val storage = system.settings.config.getString("eventuate.snapshot.storage")
  private val storeClass = system.settings.config.getString(s"eventuate.snapshot.$storage.class-fqn")

  def storeOf(logId: String): SnapshotStore = {
    // todo
    system
      .dynamicAccess
      .createInstanceFor[SnapshotStore](storeClass, List(classOf[ActorSystem] -> system, classOf[String] -> logId))
      .get
  }
}
