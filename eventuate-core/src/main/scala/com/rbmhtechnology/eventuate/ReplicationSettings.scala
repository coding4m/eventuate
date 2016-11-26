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

import java.util.concurrent.TimeUnit

import com.typesafe.config.Config

import scala.concurrent.duration._

/**
 * @author siuming
 */
class ReplicationSettings(config: Config) {

  val writeBatchSize: Int =
    config.getInt("eventuate.log.write-batch-size")

  val writeTimeout: FiniteDuration =
    config.getDuration("eventuate.log.write-timeout", TimeUnit.MILLISECONDS).millis

  val readTimeout: FiniteDuration =
    config.getDuration("eventuate.log.read-timeout", TimeUnit.MILLISECONDS).millis

  val remoteReadTimeout: FiniteDuration =
    config.getDuration("eventuate.log.replication.remote-read-timeout", TimeUnit.MILLISECONDS).millis

  val remoteScanLimit: Int =
    config.getInt("eventuate.log.replication.remote-scan-limit")

  val retryDelay: FiniteDuration =
    config.getDuration("eventuate.log.replication.retry-delay", TimeUnit.MILLISECONDS).millis

  val failureDetectionLimit: FiniteDuration =
    config.getDuration("eventuate.log.replication.failure-detection-limit", TimeUnit.MILLISECONDS).millis

  val acceptorDispatcher: String =
    "eventuate.log.replication.acceptor-dispatcher"

  val controllerDispatcher: String =
    "eventuate.log.replication.controller-dispatcher"

  require(
    failureDetectionLimit >= remoteReadTimeout + retryDelay,
    s"""
       |eventuate.log.replication.failure-detection-limit ($failureDetectionLimit) must be at least the sum of
       |eventuate.log.replication.retry-delay ($retryDelay) and
       |eventuate.log.replication.remote-read-timeout ($remoteReadTimeout)
   """.stripMargin)
}
