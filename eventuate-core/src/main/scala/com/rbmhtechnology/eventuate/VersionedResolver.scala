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
  def firstWriteWins[S]: VersionedResolver[S] = apply[S]((v1, v2) => v1.vectorTimestamp < v2.vectorTimestamp)

  /**
   * last write wins model.
   * @tparam S
   * @return
   */
  def lastWriteWins[S]: VersionedResolver[S] = apply[S]((v1, v2) => v2.vectorTimestamp < v1.vectorTimestamp)

  def apply[S](lt: (Versioned[S], Versioned[S]) => Boolean): VersionedResolver[S] = new VersionedResolver[S] {
    override def select(versions: Seq[Versioned[S]]) = versions.sortWith(lt).head
  }
}
trait VersionedResolver[S] {
  def select(versions: Seq[Versioned[S]]): Versioned[S]
}
