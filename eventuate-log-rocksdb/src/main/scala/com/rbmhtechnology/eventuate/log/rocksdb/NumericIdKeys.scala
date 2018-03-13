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

package com.rbmhtechnology.eventuate.log.rocksdb

import com.rbmhtechnology.eventuate.log.rocksdb.EventKeys.{ longBytes, stringBytes }

/**
 * @author siuming
 */
private[rocksdb] trait NumericIdKeys {
  val IdSequence = "$$SEQUENCE$$"
  val IdSequenceBytes = stringBytes(IdSequence)

  val IdSequenceInc = 1
  val IdSequenceIncBytes = longBytes(IdSequenceInc)
}
private[rocksdb] object NumericIdKeys extends NumericIdKeys