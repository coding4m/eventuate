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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.{ Function => JFunction }
import java.util.{ Set => JSet }

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import com.rbmhtechnology.eventuate.EndpointFilters.NoFilters
import com.rbmhtechnology.eventuate.EventsourcingProtocol.{ Delete, DeleteFailure, DeleteSuccess }
import com.rbmhtechnology.eventuate.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.ReplicationProtocol.ReplicationEndpointInfo
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.matching.Regex

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

  val activeTimeout: FiniteDuration =
    config.getDuration("eventuate.log.replication.active-timeout", TimeUnit.MILLISECONDS).millis

  val recoverTimeout: FiniteDuration =
    config.getDuration("eventuate.log.replication.recover-timeout", TimeUnit.MILLISECONDS).millis

  val retryDelay: FiniteDuration =
    config.getDuration("eventuate.log.replication.retry-delay", TimeUnit.MILLISECONDS).millis

  val failureDetectionLimit: FiniteDuration =
    config.getDuration("eventuate.log.replication.failure-detection-limit", TimeUnit.MILLISECONDS).millis

  val acceptorDispatcher: String =
    "eventuate.log.replication.acceptor-dispatcher"

  val controllerDispatcher: String =
    "eventuate.log.replication.controller-dispatcher"

  require(failureDetectionLimit >= remoteReadTimeout + retryDelay, s"""
     |eventuate.log.replication.failure-detection-limit ($failureDetectionLimit) must be at least the sum of
     |eventuate.log.replication.retry-delay ($retryDelay) and
     |eventuate.log.replication.remote-read-timeout ($remoteReadTimeout)
   """.stripMargin)
}

object ReplicationEndpoint {

  /**
   * Default log name.
   */
  val DefaultLogName: String = "default"

  /**
   * Default application name.
   */
  val DefaultApplicationName: String = "default"

  /**
   * Default application version.
   */
  val DefaultApplicationVersion: ApplicationVersion = ApplicationVersion()

  /**
   * Published to the actor actorSystem's event stream if a remote log is available.
   */
  case class Available(endpointId: String, logName: String)

  /**
   * Published to the actor actorSystem's event stream if a remote log is unavailable.
   */
  case class Unavailable(endpointId: String, logName: String, causes: Seq[Throwable])

  /**
   * Matches a string of format "<hostname>:<port>".
   */
  private object Address {
    //    def unapply(s: String): Option[(String, Int)] = {
    //      val hp = s.split(":")
    //      Some((hp(0), hp(1).toInt))
    //    }

    val hostAndPort = new Regex("([^:]+):(.+)")
    val hostAndPortWithName = new Regex("([^@]+)@([^:]+):(.+)")

    def unapply(s: String): Option[(String, String, Int)] = s match {
      case hostAndPort(host, port) =>
        Some((ReplicationConnection.DefaultRemoteSystemName, host, port.toInt))
      case hostAndPortWithName(name, host, port) =>
        Some((name, host, port.toInt))
    }
  }

  /**
   * Creates a [[ReplicationEndpoint]] with a single event log with name [[DefaultLogName]]. The
   * replication endpoint id and replication connections must be configured as follows in `application.conf`:
   *
   * {{{
   *   eventuate.endpoint.id = "endpoint-id"
   *   eventuate.endpoint.connections = ["host-1:port-1", "host-2:port-2", ..., "host-n:port-n"]
   * }}}
   *
   * Optionally, the `applicationName` and `applicationVersion` of a replication endpoint can be
   * configured with e.g.
   *
   * {{{
   *   eventuate.endpoint.application.name = "my-app"
   *   eventuate.endpoint.application.version = "1.2"
   * }}}
   *
   * @param logFactory Factory of log actor `Props`. The `String` parameter of the factory is a unique
   *                   log id generated by this endpoint. The log actor must be assigned this log id.
   */
  def apply(logFactory: String => Props)(implicit system: ActorSystem): ReplicationEndpoint = {
    val config = system.settings.config
    val connections = config.getStringList("eventuate.endpoint.connections").asScala.toSet[String].map {
      case Address(name, host, port) => ReplicationConnection(host, port, name)
    }
    val connectionRoles = config.getStringList("eventuate.endpoint.connection-roles").asScala.toSet
    apply(logFactory, connections, connectionRoles)
  }

  /**
   * Creates a [[ReplicationEndpoint]] with a single event log with name [[DefaultLogName]]. The
   * replication endpoint id must be configured as follows in `application.conf`:
   *
   * {{{
   *   eventuate.endpoint.id = "endpoint-id"
   * }}}
   *
   * Optionally, the `applicationName` and `applicationVersion` of a replication endpoint can be
   * configured with e.g.
   *
   * {{{
   *   eventuate.endpoint.application.name = "my-app"
   *   eventuate.endpoint.application.version = "1.2"
   * }}}
   *
   * @param logFactory Factory of log actor `Props`. The `String` parameter of the factory is a unique
   *                   log id generated by this endpoint. The log actor must be assigned this log id.
   * @param connections Replication connections to other replication endpoints.
   */
  def apply(logFactory: String => Props, connections: Set[ReplicationConnection])(implicit system: ActorSystem): ReplicationEndpoint = {
    apply(logFactory, connections, Set.empty)(system)
  }

  /**
   * Creates a [[ReplicationEndpoint]] with a single event log with name [[DefaultLogName]]. The
   * replication endpoint id must be configured as follows in `application.conf`:
   *
   * {{{
   *   eventuate.endpoint.id = "endpoint-id"
   * }}}
   *
   * Optionally, the `applicationName` and `applicationVersion` of a replication endpoint can be
   * configured with e.g.
   *
   * {{{
   *   eventuate.endpoint.application.name = "my-app"
   *   eventuate.endpoint.application.version = "1.2"
   * }}}
   *
   * @param logFactory Factory of log actor `Props`. The `String` parameter of the factory is a unique
   *                   log id generated by this endpoint. The log actor must be assigned this log id.
   * @param connections Replication connections to other replication endpoints.
   */
  def apply(logFactory: String => Props, connections: Set[ReplicationConnection], connectionRoles: Set[String])(implicit system: ActorSystem): ReplicationEndpoint = {
    val config = system.settings.config

    val endpointId = config.getString("eventuate.endpoint.id")
    val endpointLogs = config.getStringList("eventuate.endpoint.logs").asScala.toSet

    val applicationName =
      if (config.hasPath("eventuate.endpoint.application.name")) config.getString("eventuate.endpoint.application.name")
      else DefaultApplicationName

    val applicationVersion =
      if (config.hasPath("eventuate.endpoint.application.version")) ApplicationVersion(config.getString("eventuate.endpoint.application.version"))
      else DefaultApplicationVersion

    new ReplicationEndpoint(endpointId, endpointLogs, logFactory, connections, connectionRoles, applicationName = applicationName, applicationVersion = applicationVersion)(system)
  }

  /**
   * Java API that creates a [[ReplicationEndpoint]].
   *
   * Creates a [[ReplicationEndpoint]] with a single event log with name [[DefaultLogName]]. The
   * replication endpoint id and replication connections must be configured as follows in `application.conf`:
   *
   * {{{
   *   eventuate.endpoint.id = "endpoint-id"
   *   eventuate.endpoint.connections = ["host-1:port-1", "host-2:port-2", ..., "host-n:port-n"]
   * }}}
   *
   * Optionally, the `applicationName` and `applicationVersion` of a replication endpoint can be
   * configured with e.g.
   *
   * {{{
   *   eventuate.endpoint.application.name = "my-app"
   *   eventuate.endpoint.application.version = "1.2"
   * }}}
   *
   * @param logFactory Factory of log actor `Props`. The `String` parameter of the factory is a unique
   *                   log id generated by this endpoint. The log actor must be assigned this log id.
   */
  def create(logFactory: JFunction[String, Props], system: ActorSystem) =
    apply(id => logFactory.apply(id))(system)

  /**
   * Java API that creates a [[ReplicationEndpoint]].
   *
   * Creates a [[ReplicationEndpoint]] with a single event log with name [[DefaultLogName]]. The
   * replication endpoint id must be configured as follows in `application.conf`:
   *
   * {{{
   *   eventuate.endpoint.id = "endpoint-id"
   * }}}
   *
   * Optionally, the `applicationName` and `applicationVersion` of a replication endpoint can be
   * configured with e.g.
   *
   * {{{
   *   eventuate.endpoint.application.name = "my-app"
   *   eventuate.endpoint.application.version = "1.2"
   * }}}
   *
   * @param logFactory Factory of log actor `Props`. The `String` parameter of the factory is a unique
   *                   log id generated by this endpoint. The log actor must be assigned this log id.
   * @param connections Replication connections to other replication endpoints.
   */
  def create(logFactory: JFunction[String, Props], connections: JSet[ReplicationConnection], system: ActorSystem) =
    apply(id => logFactory.apply(id), connections.asScala.toSet)(system)

  /**
   * Java API that creates a [[ReplicationEndpoint]].
   *
   * Creates a [[ReplicationEndpoint]] with a single event log with name [[DefaultLogName]]. The
   * replication endpoint id must be configured as follows in `application.conf`:
   *
   * {{{
   *   eventuate.endpoint.id = "endpoint-id"
   * }}}
   *
   * Optionally, the `applicationName` and `applicationVersion` of a replication endpoint can be
   * configured with e.g.
   *
   * {{{
   *   eventuate.endpoint.application.name = "my-app"
   *   eventuate.endpoint.application.version = "1.2"
   * }}}
   *
   * @param logFactory Factory of log actor `Props`. The `String` parameter of the factory is a unique
   *                   log id generated by this endpoint. The log actor must be assigned this log id.
   * @param connections Replication connections to other replication endpoints.
   */
  def create(logFactory: JFunction[String, Props], connections: JSet[ReplicationConnection], connectionRoles: JSet[String], system: ActorSystem) =
    apply(id => logFactory.apply(id), connections.asScala.toSet, connectionRoles.asScala.toSet)(system)

  /**
   * Java API that creates a [[ReplicationEndpoint]].
   *
   * @param id Unique replication endpoint id.
   * @param logNames Names of the event logs managed by this replication endpoint.
   * @param logFactory Factory of log actor `Props`. The `String` parameter of the factory is a unique
   *                   log id generated by this endpoint. The log actor must be assigned this log id.
   * @param connections Replication connections to other replication endpoints.
   * @param endpointFilters Replication filters applied to incoming replication read requests
   * @param applicationName Name of the application that creates this replication endpoint.
   * @param applicationVersion Version of the application that creates this replication endpoint.
   */
  def create(
    id: String,
    logNames: JSet[String],
    logFactory: JFunction[String, Props],
    connections: JSet[ReplicationConnection],
    endpointFilters: EndpointFilters,
    applicationName: String,
    applicationVersion: ApplicationVersion,
    system: ActorSystem
  ): ReplicationEndpoint =
    new ReplicationEndpoint(id, logNames.asScala.toSet, id => logFactory.apply(id), connections.asScala.toSet, Set.empty, endpointFilters, applicationName, applicationVersion)(system)
}

/**
 * A replication endpoint connects to other replication endpoints for replicating events. Events are
 * replicated from the connected endpoints to this endpoint. The connected endpoints are ''replication
 * sources'', this endpoint is a ''replication target''. To setup bi-directional replication, the other
 * replication endpoints must additionally setup replication connections to this endpoint.
 *
 * A replication endpoint manages one or more event logs. Event logs are indexed by name. Events are
 * replicated only between event logs with matching names.
 *
 * If `applicationName` equals that of a replication source, events are only replicated if `applicationVersion`
 * is greater than or equal to that of the replication source. This is a simple mechanism to support
 * incremental version upgrades of replicated applications where each replica can be upgraded individually
 * without shutting down other replicas. This avoids permanent state divergence during upgrade which may
 * occur if events are replicated from replicas with higher version to those with lower version. If
 * `applicationName` does not equal that of a replication source, events are always replicated, regardless
 * of the `applicationVersion` value.
 *
 * @param id Unique replication endpoint id.
 * @param logNames Names of the event logs managed by this replication endpoint.
 * @param logFactory Factory of log actor `Props`. The `String` parameter of the factory is a unique
 *                   log id generated by this endpoint. The log actor must be assigned this log id.
 * @param connections Replication connections to other replication endpoints.
 * @param endpointFilters Replication filters applied to incoming replication read requests
 * @param applicationName Name of the application that creates this replication endpoint.
 * @param applicationVersion Version of the application that creates this replication endpoint.
 */
class ReplicationEndpoint(
  val id: String,
  val logNames: Set[String],
  val logFactory: String => Props,
  val connections: Set[ReplicationConnection],
  val connectionRoles: Set[String] = Set.empty,
  val endpointFilters: EndpointFilters = NoFilters,
  val applicationName: String = ReplicationEndpoint.DefaultApplicationName,
  val applicationVersion: ApplicationVersion = ReplicationEndpoint.DefaultApplicationVersion)(implicit val system: ActorSystem) {

  private val active: AtomicBoolean =
    new AtomicBoolean(false)

  /**
   * The actor actorSystem's replication settings.
   */
  val settings =
    new ReplicationSettings(system.settings.config)

  /**
   * The log actors managed by this endpoint, indexed by their name.
   */
  val logs: Map[String, ActorRef] =
    logNames.map(logName => logName -> system.actorOf(logFactory(logId(logName)), logId(logName))).toMap

  // lazy to make sure concurrently running (created actors) do not access null-reference
  // https://github.com/RBMHTechnology/eventuate/issues/183
  private[eventuate] lazy val acceptor: ActorRef =
    system.actorOf(Acceptor.props(this), name = Acceptor.Name)
  acceptor // make sure acceptor is started

  private[eventuate] lazy val controller: ActorRef =
    system.actorOf(Controller.props(this), name = Controller.Name)
  controller

  /**
   * Returns the unique log id for given `logName`.
   */
  def logId(logName: String): String =
    ReplicationEndpointInfo.logId(id, logName)

  /**
   * Activates this endpoint by starting event replication from remote endpoints to this endpoint.
   */
  def activate(): Future[Unit] = if (active.compareAndSet(false, true)) {
    import system.dispatcher
    val replication = new Replication(this)
    for {
      connections <- replication.awaitReplicationConnections
    } yield {
      replication.activateReplicationConnections(connections)
    }
  } else Future.failed(new IllegalStateException("Recovery running or endpoint already activated"))

  /**
   * Runs an asynchronous disaster recovery procedure. This procedure recovers this endpoint in case of total or
   * partial event loss. Partial event loss means event loss from a given sequence number upwards (for example,
   * after having installed a storage backup). Recovery copies events from directly connected remote endpoints back
   * to this endpoint and automatically removes invalid snapshots. A snapshot is invalid if it covers events that
   * have been lost.
   *
   * This procedure requires that event replication between this and directly connected endpoints is bi-directional
   * and that these endpoints are available during recovery. After successful recovery the endpoint is automatically
   * activated. A failed recovery completes with a [[RecoveryException]] and must be retried. Activating this endpoint
   * without having successfully recovered from partial or total event loss may result in inconsistent replica states.
   *
   * Running a recovery on an endpoint that didn't loose events has no effect but may still fail due to unavailable
   * replication partners, for example. In this case, a recovery retry can be omitted if the `partialUpdate` field
   * of [[RecoveryException]] is set to `false`.
   */
  def recover(): Future[Unit] = if (active.compareAndSet(false, true)) {
    import system.dispatcher

    def recoveryFailure[U](partialUpdate: Boolean): PartialFunction[Throwable, Future[U]] = {
      case t =>
        Future.failed(new RecoveryException(t, partialUpdate))
    }

    // Disaster recovery is executed in 3 steps:
    // 1. synchronize metadata to
    //    - reset replication progress of remote sites and
    //    - determine after disaster progress of remote sites
    // 2. Recover events from unfiltered links
    // 3. Recover events from filtered links
    // 4. Adjust the sequence numbers of local logs to their version vectors
    // unfiltered links are recovered first to ensure that no events are recovered from a filtered connection
    // where the causal predecessor is not yet recovered (from an unfiltered connection)
    // as causal predecessors cannot be written after their successors to the event log.
    // The sequence number of an event log needs to be adjusted if not all events could be
    // recovered as otherwise it could be less then the corresponding entriy in the
    // log's version vector
    val recovery = new Recovery(this)
    for {
      connections <- recovery.awaitReplicationConnections.recoverWith(recoveryFailure(partialUpdate = false))
      endpointInfo <- recovery.readEndpointInfo.recoverWith(recoveryFailure(partialUpdate = false))
      _ = logRecoverySequenceNrs(connections, endpointInfo)
      recoveryLinks <- recovery.adjustReplicationProgresses(connections, endpointInfo).recoverWith(recoveryFailure(partialUpdate = false))
      unfilteredLinks = recoveryLinks.filterNot(recovery.isFilteredLink)
      _ = logRecoveryLinks(unfilteredLinks, "unfiltered")
      _ <- recovery.recoverLinks(unfilteredLinks).recoverWith(recoveryFailure(partialUpdate = true))
      filteredLinks = recoveryLinks.filter(recovery.isFilteredLink)
      _ = logRecoveryLinks(filteredLinks, "filtered")
      _ <- recovery.recoverLinks(filteredLinks).recoverWith(recoveryFailure(partialUpdate = true))
      _ <- recovery.adjustEventLogClocks.recoverWith(recoveryFailure(partialUpdate = true))
    } yield recovery.recoverCompleted()
  } else Future.failed(new IllegalStateException("Recovery running or endpoint already activated"))

  private def logRecoverySequenceNrs(connections: Set[ReplicationConnection], info: ReplicationEndpointInfo): Unit = {
    system.log.info(
      "Disaster recovery initiated for endpoint {}. Sequence numbers of local logs are: {}",
      info.endpointId, sequenceNrsLogString(info))
    system.log.info(
      "Need to reset replication progress stored at remote replicas {}",
      connections.map(replicationAcceptor).mkString(","))
  }

  private def logRecoveryLinks(links: Set[RecoveryLink], linkType: String): Unit = {
    system.log.info(
      "Start recovery for {} links: (from remote source log (target seq no) -> local target log (initial seq no))\n{}",
      linkType, links.map(l => s"(${l.replicationLink.source.logId} (${l.remoteSequenceNr}) -> ${l.replicationLink.target.logName} (${l.localSequenceNr}))").mkString(", "))
  }

  private def sequenceNrsLogString(info: ReplicationEndpointInfo): String =
    info.logSequenceNrs.map { case (logName, sequenceNr) => s"$logName:$sequenceNr" } mkString ","

  /**
   * Delete events from a local log identified by `logName` with a sequence number less than or equal to
   * `toSequenceNr`. Deletion is split into logical deletion and physical deletion. Logical deletion is
   * supported by any storage backend and ensures that deleted events are not replayed any more. It has
   * immediate effect. Logically deleted events can still be replicated to remote [[ReplicationEndpoint]]s.
   * They are only physically deleted if the storage backend supports that (currently LevelDB only). Furthermore,
   * physical deletion only starts after all remote replication endpoints identified by `remoteEndpointIds`
   * have successfully replicated these events. Physical deletion is implemented as reliable background
   * process that survives event log restarts.
   *
   * Use with care! When events are physically deleted they cannot be replicated any more to new replication
   * endpoints (i.e. those that were unknown at the time of deletion). Also, a location with deleted events
   * may not be suitable any more for disaster recovery of other locations.
   *
   * @param logName Events are deleted from the local log with this name.
   * @param toSequenceNr Sequence number up to which events shall be deleted (inclusive).
   * @param remoteEndpointIds A set of remote [[ReplicationEndpoint]] ids that must have replicated events
   *                          to their logs before they are allowed to be physically deleted at this endpoint.
   * @return The sequence number up to which events have been logically deleted. When the returned `Future`
   *         completes logical deletion is effective. The returned sequence number can differ from the requested
   *         one, if:
   *
   *         - the log's current sequence number is smaller than the requested number. In this case the current
   *          sequence number is returned.
   *         - there was a previous successful deletion request with a higher sequence number. In this case that
   *          number is returned.
   */
  def delete(logName: String, toSequenceNr: Long, remoteEndpointIds: Set[String]): Future[Long] = {
    import system.dispatcher
    implicit val timeout = Timeout(settings.writeTimeout)
    (logs(logName) ? Delete(toSequenceNr, remoteEndpointIds.map(ReplicationEndpointInfo.logId(_, logName)))).flatMap {
      case DeleteSuccess(deletedTo) => Future.successful(deletedTo)
      case DeleteFailure(ex)        => Future.failed(ex)
    }
  }

  /**
   * Creates [[ReplicationTarget]] for given `logName`.
   */
  private[eventuate] def target(logName: String): ReplicationTarget = {
    ReplicationTarget(this, logName, logId(logName), logs(logName))
  }

  private[eventuate] def source(
    logName: String,
    connection: ReplicationConnection,
    endpointInfo: ReplicationEndpointInfo): ReplicationSource = {
    ReplicationSource(
      endpointInfo.endpointId, logName, endpointInfo.logId(logName), replicationAcceptor(connection)
    )
  }

  private[eventuate] def replicationLinks(
    connection: ReplicationConnection,
    endpointInfo: ReplicationEndpointInfo): Set[ReplicationLink] = {
    replicationLogs(endpointInfo).map { logName =>
      ReplicationLink(source(logName, connection, endpointInfo), target(logName))
    }
  }

  /**
   * Returns all log names this endpoint and `endpointInfo` have in common.
   */
  private[eventuate] def replicationLogs(endpointInfo: ReplicationEndpointInfo) =
    this.logNames.intersect(endpointInfo.logNames)

  private[eventuate] def replicationAcceptor(connection: ReplicationConnection): ActorSelection = {
    import connection._
    val actor = Acceptor.Name
    val protocol = system match {
      case sys: ExtendedActorSystem => sys.provider.getDefaultAddress.protocol
      case sys                      => "akka.tcp"
    }

    system.actorSelection(s"""$protocol://$name@$host:$port/user/$actor""")
  }
}

/**
 * [[EndpointFilters]] computes a [[ReplicationFilter]] that shall be applied to a
 * replication read request that replicates from a source log (defined by ``sourceLogName``)
 * to a target log (defined by ``targetLogId``).
 */
trait EndpointFilters {
  def filterFor(targetLogId: String, sourceLogName: String): ReplicationFilter
}

object EndpointFilters {
  private class CombiningEndpointFilters(
    targetFilters: Map[String, ReplicationFilter],
    sourceFilters: Map[String, ReplicationFilter],
    targetSourceCombinator: (ReplicationFilter, ReplicationFilter) => ReplicationFilter
  ) extends EndpointFilters {

    override def filterFor(targetLogId: String, sourceLogName: String): ReplicationFilter = {
      (targetFilters.get(targetLogId), sourceFilters.get(sourceLogName)) match {
        case (None, Some(sourceFilter))               => sourceFilter
        case (Some(targetFilter), None)               => targetFilter
        case (None, None)                             => NoFilter
        case (Some(targetFilter), Some(sourceFilter)) => targetSourceCombinator(targetFilter, sourceFilter)
      }
    }
  }

  /**
   * Creates an [[EndpointFilters]] instance that computes a [[ReplicationFilter]] for a replication read request
   * from a source log to a target log by and-combining target and source filters when given in the provided [[Map]]s.
   * If only source or target filter is given that is returned. If no filter is given [[com.rbmhtechnology.eventuate.ReplicationFilter.NoFilter]] is returned.
   * A typical use case is that target specific filters are and-combined with a (default) source filter.
   *
   * @param targetFilters maps target log ids to the [[ReplicationFilter]] that shall be applied when replicating from a source log to this target log
   * @param sourceFilters maps source log names to the [[ReplicationFilter]] that shall be applied when replicating from this source log to any target log
   */
  def targetAndSourceFilters(targetFilters: Map[String, ReplicationFilter], sourceFilters: Map[String, ReplicationFilter]): EndpointFilters =
    new CombiningEndpointFilters(targetFilters, sourceFilters, _ and _)

  /**
   * Creates an [[EndpointFilters]] instance that computes a [[ReplicationFilter]] for a replication read request
   * from a source log to a target log by returning a target filter when given in `targetFilters`. If only
   * a source filter is given in `sourceFilters` that is returned otherwise [[com.rbmhtechnology.eventuate.ReplicationFilter.NoFilter]] is returned.
   * A typical use case is that (more privileged) remote targets may replace a (default) source filter with a target-specific filter.
   *
   * @param targetFilters maps target log ids to the [[ReplicationFilter]] that shall be applied when replicating from a source log to this target log
   * @param sourceFilters maps source log names to the [[ReplicationFilter]] that shall be applied when replicating from this source log to any target log
   */
  def targetOverwritesSourceFilters(targetFilters: Map[String, ReplicationFilter], sourceFilters: Map[String, ReplicationFilter]): EndpointFilters =
    new CombiningEndpointFilters(targetFilters, sourceFilters, _ leftIdentity _)

  /**
   * Creates an [[EndpointFilters]] instance that computes a [[ReplicationFilter]] for a replication read request
   * from a source log to any target log by returning the source filter when given in `sourceFilters` or
   * [[com.rbmhtechnology.eventuate.ReplicationFilter.NoFilter]] otherwise.
   *
   * @param sourceFilters maps source log names to the [[ReplicationFilter]] that shall be applied when replicating from this source log to any target log
   */
  def sourceFilters(sourceFilters: Map[String, ReplicationFilter]): EndpointFilters = new EndpointFilters {
    override def filterFor(targetLogId: String, sourceLogName: String): ReplicationFilter =
      sourceFilters.getOrElse(sourceLogName, NoFilter)
  }

  /**
   * Creates an [[EndpointFilters]] instance that computes a [[ReplicationFilter]] for a replication read request
   * to a target log by returning the target filter when given in `targetFilters` or
   * [[com.rbmhtechnology.eventuate.ReplicationFilter.NoFilter]] otherwise.
   *
   * @param targetFilters maps target log ids to the [[ReplicationFilter]] that shall be applied when replicating from a source log to this target log
   */
  def targetFilters(targetFilters: Map[String, ReplicationFilter]): EndpointFilters = new EndpointFilters {
    override def filterFor(targetLogId: String, sourceLogName: String): ReplicationFilter =
      targetFilters.getOrElse(targetLogId, NoFilter)
  }

  /**
   * An [[EndpointFilters]] instance that always returns [[com.rbmhtechnology.eventuate.ReplicationFilter.NoFilter]]
   * independent from source/target logs of the replication read request.
   */
  val NoFilters: EndpointFilters = new EndpointFilters {
    override def filterFor(targetLogId: String, sourceLogName: String): ReplicationFilter = NoFilter
  }
}

