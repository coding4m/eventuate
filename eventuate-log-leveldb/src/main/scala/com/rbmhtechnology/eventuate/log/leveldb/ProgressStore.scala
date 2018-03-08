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

package com.rbmhtechnology.eventuate.log.leveldb

import org.iq80.leveldb.{ DB, ReadOptions, WriteOptions }

import scala.annotation.tailrec

private[leveldb] class ProgressStore(val leveldb: DB, val writeOptions: WriteOptions, classifier: Int) extends LeveldbBatchLayer {
  import ProgressKeys._

  def writeProgresses(progresses: Map[String, Long]): Unit = withBatch { batch =>
    progresses.foreach(it => batch.put(progressKeyBytes(classifier, it._1), longBytes(it._2)))
    leveldb.write(batch, writeOptions)
  }

  def readProgress(logId: String): Long = {
    val progress = leveldb.get(progressKeyBytes(classifier, logId))
    if (progress == null) 0L else longFromBytes(progress)
  }

  def readProgresses(): Map[String, Long] = {
    withIterator(new ReadOptions().verifyChecksums(false).snapshot(leveldb.getSnapshot)) { it =>
      it.seekToFirst()
      readProgresses(Map.empty[String, Long], it.seekToFirst())
    }
  }

  @tailrec
  private def readProgresses(progresses: Map[String, Long], iter: ProgressIterator): Map[String, Long] = {
    if (!iter.hasNext) progresses else {
      val entry = iter.next()
      readProgresses(progresses + (entry.id -> entry.sequenceNr), iter)
    }
  }

  private def withIterator[T](options: ReadOptions)(body: ProgressIterator => T): T = {
    val iterator = ProgressIterator(leveldb.iterator(options), classifier)
    try {
      body(iterator)
    } finally {
      iterator.close()
      options.snapshot().close()
    }
  }
}
