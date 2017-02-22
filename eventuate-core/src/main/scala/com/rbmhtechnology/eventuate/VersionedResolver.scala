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

/**
 * @author siuming
 */
object VersionedResolver {

  /**
   * first write wins model.
   * @tparam S
   * @return
   */
  def firstWriteWins[S]: VersionedResolver[S] = VersionedResolver[S]((v1, v2) => {
    if (v1.systemTimestamp == v2.systemTimestamp) v1.creator < v2.creator else v1.systemTimestamp < v2.systemTimestamp
  })

  /**
   * last write wins model.
   * @tparam S
   * @return
   */
  def lastWriteWins[S]: VersionedResolver[S] = VersionedResolver[S]((v1, v2) => {
    if (v1.systemTimestamp == v2.systemTimestamp) v2.creator < v1.creator else v2.systemTimestamp < v1.systemTimestamp
  })

  def apply[S](lt: (Versioned[S], Versioned[S]) => Boolean): VersionedResolver[S] = new VersionedResolver[S] {
    override def select(versions: Seq[Versioned[S]]) = versions.sortWith(lt).head
  }
}
trait VersionedResolver[S] {
  def select(versions: Seq[Versioned[S]]): Versioned[S]
}
