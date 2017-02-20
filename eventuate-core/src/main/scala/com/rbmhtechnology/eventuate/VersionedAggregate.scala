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

import java.util.function.BiFunction
import java.util.{ List => JList, Optional => JOption }

import com.rbmhtechnology.eventuate.VersionedAggregate._

import scala.annotation.implicitNotFound
import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._
import scala.util._

/**
 * Manages concurrent versions of an event-sourced aggregate.
 *
 * @param id Aggregate id
 * @param cmdHandler Command handler
 * @param evtHandler Event handler
 * @param aggregate Aggregate.
 * @tparam S Aggregate type.
 * @tparam C Command type.
 * @tparam E Event type.
 */
case class VersionedAggregate[S, C: DomainCmd, E: DomainEvt](
  id: String,
  cmdHandler: (S, C) => Try[E],
  evtHandler: (S, E) => S,
  aggregate: Option[ConcurrentVersions[S, E]] = None) {

  @implicitNotFound("No DomainCmd for VersionedAggregate.")
  val C = implicitly[DomainCmd[C]]
  @implicitNotFound("No DomainEvt for VersionedAggregate.")
  val E = implicitly[DomainEvt[E]]

  /**
   * Java API that returns the [[aggregate]].
   */
  def getAggregate: JOption[ConcurrentVersions[S, E]] =
    aggregate.asJava

  /**
   * Java API that returns the [[versions]].
   */
  def getVersions: JList[Versioned[S]] =
    versions.asJava

  def getHeadVersion: Versioned[S] =
    versions.head

  def getLastVersion: Versioned[S] =
    versions.last

  /**
   * Java API.
   *
   * Calls the given `handler` with the result of the validation.
   *
   * @param handler handler with callbacks for validation result.
   */
  def validateCreate(cmd: C, handler: ResultHandler[E]): Unit =
    handler.accept(validateCreate(cmd))

  /**
   * Java API.
   *
   * Calls the given `handler` with the result of the validation.
   *
   * @param handler handler with callbacks for validation result.
   */
  def validateUpdate(cmd: C, handler: ResultHandler[E]): Unit =
    handler.accept(validateUpdate(cmd))

  /**
   * Java API.
   *
   * Calls the given `handler` with the result of the validation.
   *
   * @param handler handler with callbacks for validation result.
   */
  def validateResolve(selected: Int, origin: String, handler: ResultHandler[Resolved]): Unit =
    handler.accept(validateResolve(selected, origin))

  def withAggregate(aggregate: ConcurrentVersions[S, E]): VersionedAggregate[S, C, E] =
    copy(aggregate = Some(aggregate))

  def versions: Seq[Versioned[S]] = aggregate match {
    case Some(versions) =>
      versions.all
    case None =>
      Seq.empty
  }

  def headVersion: Versioned[S] =
    versions.head

  def lastVersion: Versioned[S] =
    versions.last

  def validateCreate(
    cmd: C,
    onViolated: (String) => Throwable = id => new AggregateAlreadyExistsException(id)): Try[E] = aggregate match {
    case None    => cmdHandler(null.asInstanceOf[S] /* FIXME */ , cmd)
    case Some(_) => Failure(onViolated(id))
  }

  def validateUpdate(
    cmd: C,
    onViolated: (String) => Throwable = id => new AggregateDoesNotExistException(id),
    onConflicted: (String, Seq[Versioned[S]]) => Throwable = (id, versioneds) => new ConflictDetectedException[S](id, versioneds)): Try[E] = aggregate match {
    case None                                => Failure(onViolated(id))
    case Some(versions) if versions.conflict => Failure(onConflicted(id, versions.all))
    case Some(versions)                      => cmdHandler(versions.all.head.value, cmd)
  }

  def validateResolve(
    selected: Int,
    origin: String,
    onViolated: (String) => Throwable = id => new AggregateDoesNotExistException(id),
    onConflictNotDetected: (String) => Throwable = id => new ConflictNotDetectedException(id),
    onConflictResolutionRejected: (String, String, String) => Throwable = (id, owner, origin) => new ConflictResolutionRejectedException(id, owner, origin)): Try[Resolved] = aggregate match {
    case None                                       => Failure(onViolated(id))
    case Some(versions) if !versions.conflict       => Failure(onConflictNotDetected(id))
    case Some(versions) if versions.owner != origin => Failure(onConflictResolutionRejected(id, versions.owner, origin))
    case Some(versions)                             => Success(Resolved(id, versions.all(selected).vectorTimestamp, origin))
  }

  def handleCreated(evt: E, timestamp: VectorTime, sequenceNr: Long): VersionedAggregate[S, C, E] = {
    val versions = aggregate match {
      case None     => ConcurrentVersionsTree(evtHandler).withOwner(E.origin(evt))
      // concurrent create
      case Some(cv) => cv.withOwner(priority(cv.owner, E.origin(evt)))
    }
    copy(aggregate = Some(versions.update(evt, timestamp)))
  }

  def handleUpdated(evt: E, timestamp: VectorTime, sequenceNr: Long): VersionedAggregate[S, C, E] = {
    copy(aggregate = aggregate.map(_.update(evt, timestamp)))
  }

  def handleResolved(evt: Resolved, updateTimestamp: VectorTime, sequenceNr: Long): VersionedAggregate[S, C, E] = {
    copy(aggregate = aggregate.map(_.resolve(evt.selected, updateTimestamp)))
  }

  def reslove(resolver: VersionedResolver[S]): VersionedAggregate[S, C, E] = {
    copy(aggregate = aggregate.map(versions => resolveIfConflict(versions, resolver)))
  }

  private def resolveIfConflict(
    versions: ConcurrentVersions[S, E],
    resolver: VersionedResolver[S]): ConcurrentVersions[S, E] = if (versions.conflict) {
    versions.resolve(resolver.select(versions.all).vectorTimestamp)
  } else versions
}

object VersionedAggregate {
  trait DomainCmd[C] {
    def origin(cmd: C): String
  }

  trait DomainEvt[E] {
    def origin(evt: E): String
  }

  case class Resolved(id: String, selected: VectorTime, origin: String = "")
  case class Resolve(id: String, selected: Int, origin: String = "") {
    /**
     * Java API that sets the origin.
     *
     * @see [[VersionedAggregate]]
     */
    def withOrigin(origin: String) = copy(origin = origin)
  }

  class AggregateNotLoadedException(id: String)
    extends Exception(s"aggregate $id not loaded")

  class AggregateAlreadyExistsException(id: String)
    extends Exception(s"aggregate $id already exists")

  class AggregateDoesNotExistException(id: String)
    extends Exception(s"aggregate $id does not exist")

  class ConflictResolutionRejectedException(id: String, origin1: String, origin2: String)
    extends Exception(s"conflict for aggregate $id can only be resolved by $origin1 but $origin2 has attempted")

  class ConflictNotDetectedException(id: String)
    extends Exception(s"conflict for aggregate $id not detected")

  class ConflictDetectedException[S](id: String, val versions: Seq[Versioned[S]])
    extends Exception(s"conflict for aggregate $id detected") {

    /**
     * Java API that returns the [[versions]].
     */
    def getVersions: JList[Versioned[S]] =
      versions.asJava
  }

  def create[S, C: DomainCmd, E: DomainEvt](
    id: String,
    cmdHandler: BiFunction[S, C, E],
    evtHandler: BiFunction[S, E, S]) =
    new VersionedAggregate[S, C, E](id, (s, c) => Try(cmdHandler.apply(s, c)), (s, e) => evtHandler.apply(s, e))

  def priority(creator1: String, creator2: String): String =
    if (creator1 < creator2) creator1 else creator2
}
