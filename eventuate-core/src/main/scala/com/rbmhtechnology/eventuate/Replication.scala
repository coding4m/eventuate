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

import akka.actor._
import akka.cluster.ClusterEvent.{ InitialStateAsEvents, MemberUp, ReachableMember, UnreachableMember }
import akka.cluster.{ Cluster, Member }
import akka.event.Logging
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.log.EventLogClock
import com.typesafe.config.Config

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.concurrent.duration._
import scala.reflect.ClassTag

/**
 * [[ReplicationEndpoint.recover]] completes with this exception if recovery fails.
 *
 * @param cause         Recovery failure cause.
 * @param partialUpdate Set to `true` if recovery already made partial updates, `false` if recovery
 *                      failed without having made partial updates to replication partners.
 */
class RecoveryException(cause: Throwable, val partialUpdate: Boolean) extends RuntimeException(cause)

private class RecoverySettings(config: Config) {
  val localReadTimeout: FiniteDuration =
    config.getDuration("eventuate.log.read-timeout", TimeUnit.MILLISECONDS).millis

  val localWriteTimeout: FiniteDuration =
    config.getDuration("eventuate.log.write-timeout", TimeUnit.MILLISECONDS).millis

  val remoteOperationRetryMax: Int =
    config.getInt("eventuate.log.recovery.remote-operation-retry-max")

  val remoteOperationRetryDelay: FiniteDuration =
    config.getDuration("eventuate.log.recovery.remote-operation-retry-delay", TimeUnit.MILLISECONDS).millis

  val remoteOperationTimeout: FiniteDuration =
    config.getDuration("eventuate.log.recovery.remote-operation-timeout", TimeUnit.MILLISECONDS).millis

  val snapshotDeletionTimeout: FiniteDuration =
    config.getDuration("eventuate.log.recovery.snapshot-deletion-timeout", TimeUnit.MILLISECONDS).millis
}

/**
 * Represents a link between a local and remote event log that are subject to disaster recovery.
 *
 * @param replicationLink  used to recover events (through replication)
 * @param localSequenceNr  sequence number of the local event log at the beginning of disaster recovery.
 * @param remoteSequenceNr Current sequence nr of the remote log
 */
private case class RecoveryLink(replicationLink: ReplicationLink, localSequenceNr: Long, remoteSequenceNr: Long)

/**
 * Provides disaster recovery primitives.
 *
 * @param endpoint endpoint to be recovered.
 * @see [[ReplicationEndpoint.recover()]]
 */
private class Recovery(endpoint: ReplicationEndpoint) {
  private val settings = new RecoverySettings(endpoint.system.settings.config)

  import Acceptor._
  import Controller._

  import endpoint.system.dispatcher
  import settings._

  private implicit val timeout = Timeout(remoteOperationTimeout)
  private implicit val scheduler = endpoint.system.scheduler

  def awaitReplicationConnections(implicit ec: ExecutionContext): Future[Set[ReplicationConnection]] = {
    implicit val timeout = Timeout(endpoint.settings.recoverTimeout)
    (endpoint.controller ? GetReplicationConnections).asInstanceOf[Future[GetReplicationConnectionsSuccess]] map {
      _.connections
    }
  }

  /**
   * Read [[ReplicationEndpointInfo]] from local [[ReplicationEndpoint]]
   */
  def readEndpointInfo: Future[ReplicationEndpointInfo] =
    readLogSequenceNrs.map(ReplicationEndpointInfo(endpoint.id, _))

  private def readLogSequenceNrs: Future[Map[String, Long]] =
    readEventLogClocks.map(_.mapValues(_.sequenceNr).view.force)

  /**
   * Reads the clocks from local event logs.
   */
  def readEventLogClocks: Future[Map[String, EventLogClock]] =
    Future.traverse(endpoint.logNames)(name => readEventLogClock(endpoint.logs(name)).map(name -> _)).map(_.toMap)

  /**
   * Synchronize sequence numbers of local logs with replication progress stored in remote replicas.
   *
   * @return a set of [[RecoveryLink]]s indicating the events that need to be recovered
   */
  def adjustReplicationProgresses(connections: Set[ReplicationConnection], localInfo: ReplicationEndpointInfo): Future[Set[RecoveryLink]] =
    Future.traverse(connections) { connection =>
      adjustReplicationProgress(endpoint.replicationAcceptor(connection), localInfo).map { remoteInfo =>
        endpoint.replicationLinks(connection, remoteInfo).map(toRecoveryLink(_, localInfo, remoteInfo))
      }
    } map (_.flatten)

  private def toRecoveryLink(
    replicationLink: ReplicationLink,
    localInfo: ReplicationEndpointInfo,
    remoteInfo: ReplicationEndpointInfo): RecoveryLink =
    RecoveryLink(replicationLink, localInfo.logSequenceNrs(replicationLink.target.logName), remoteInfo.logSequenceNrs(replicationLink.target.logName))

  private def adjustReplicationProgress(remoteAcceptor: ActorSelection, localInfo: ReplicationEndpointInfo): Future[ReplicationEndpointInfo] =
    readResult[SynchronizeReplicationProgressSuccess, SynchronizeReplicationProgressFailure, ReplicationEndpointInfo](
      Retry(remoteAcceptor.ask(SynchronizeReplicationProgress(localInfo)), remoteOperationRetryDelay, remoteOperationRetryMax), _.info, _.cause)

  /**
   * Update the locally stored replication progress of remote replicas with the sequence numbers given in ``info``.
   * Replication progress that is greater than the corresponding sequence number in ``info`` is reset to that
   */
  def synchronizeReplicationProgress(info: ReplicationEndpointInfo): Future[Unit] = {
    Future.traverse(endpoint.replicationLogs(info)) { name =>
      val logActor = endpoint.logs(name)
      val logId = info.logId(name)
      val remoteSequenceNr = info.logSequenceNrs(name)
      for {
        currentProgress <- readReplicationProgress(logActor, logId)
        _ <- if (currentProgress > remoteSequenceNr) updateReplicationMetadata(logActor, logId, remoteSequenceNr)
        else Future.successful(currentProgress)
      } yield ()
    } map (_ => ())
  }

  private def readReplicationProgress(logActor: ActorRef, logId: String): Future[Long] =
    readResult[GetReplicationProgressSuccess, GetReplicationProgressFailure, Long](
      logActor.ask(GetReplicationProgress(logId))(localReadTimeout), _.storedReplicationProgress, _.cause)

  /**
   * Sets the replication progress for the remote replicate with id `logId` to `replicationProgress`
   * and clears the cached version vector.
   */
  private def updateReplicationMetadata(logActor: ActorRef, logId: String, replicationProgress: Long): Future[Long] = {
    readResult[ReplicationWriteSuccess, ReplicationWriteFailure, Long](
      logActor.ask(ReplicationWrite(Seq.empty, Map(logId -> ReplicationMetadata(replicationProgress, VectorTime.Zero))))(localWriteTimeout), _ => replicationProgress, _.cause)
  }

  /**
   * @return `true`, if the source of the [[RecoveryLink]] did not receive all events before the disaster, i.e.
   *         the initial replication from the location to be recovered to the source of event recovery was filtered.
   */
  def isFilteredLink(link: RecoveryLink): Boolean =
    endpoint.endpointFilters.filterFor(link.replicationLink.source.logId, link.replicationLink.target.logName) != NoFilter

  /**
   * Initiates event recovery for the given [[ReplicationLink]]s. The returned [[Future]] completes when
   * all events are successfully recovered.
   */
  def recoverLinks(recoveryLinks: Set[RecoveryLink])(implicit ec: ExecutionContext): Future[Unit] = {
    if (recoveryLinks.isEmpty) {
      Future.successful(())
    } else {
      val recoveryFinishedPromise = Promise[Unit]()
      deleteSnapshots(recoveryLinks).onSuccess {
        case _ =>
          endpoint.acceptor ! Recover(recoveryLinks, recoveryFinishedPromise)
          recoveryLinks.foreach { link =>
            endpoint.controller ! ActivateReplication(link.replicationLink)
          }
      }
      recoveryFinishedPromise.future
    }
  }

  def recoverCompleted(): Unit =
    endpoint.acceptor ! RecoverCompleted

  /**
   * Deletes all invalid snapshots from local event logs. A snapshot is invalid if it covers
   * events that have been lost.
   */
  private def deleteSnapshots(links: Set[RecoveryLink]): Future[Unit] =
    Future.sequence(links.map(deleteSnapshots)).map(_ => ())

  def readEventLogClock(targetLog: ActorRef): Future[EventLogClock] =
    targetLog.ask(GetEventLogClock)(localReadTimeout).mapTo[GetEventLogClockSuccess].map(_.clock)

  private def deleteSnapshots(link: RecoveryLink): Future[Unit] =
    readResult[DeleteSnapshotsSuccess.type, DeleteSnapshotsFailure, Unit](
      endpoint.logs(link.replicationLink.target.logName).ask(DeleteSnapshots(link.localSequenceNr + 1L))(Timeout(snapshotDeletionTimeout)), _ => (), _.cause)

  /**
   * In case disaster recovery was not able to recover all events (e.g. only through a single filtered connection)
   * the local sequence no must be adjusted to the log's version vector to avoid events being
   * written in the causal past.
   */
  def adjustEventLogClocks: Future[Unit] =
    Future.traverse(endpoint.logs.values)(adjustEventLogClock).map(_ => ())

  private def adjustEventLogClock(log: ActorRef): Future[Unit] = {
    readResult[AdjustEventLogClockSuccess, AdjustEventLogClockFailure, Unit](
      log ? AdjustEventLogClock, _ => (), _.cause)
  }

  private def readResult[S: ClassTag, F: ClassTag, R](f: Future[Any], result: S => R, cause: F => Throwable): Future[R] = f.flatMap {
    case success: S => Future.successful(result(success))
    case failure: F => Future.failed(cause(failure))
  }
}

/**
 * [[ReplicationEndpoint]]-scoped singleton that receives all requests from remote endpoints. These are
 *
 *  - [[GetReplicationEndpointInfo]] requests.
 *  - [[ReplicationRead]] requests (inside [[ReplicationReadEnvelope]]s).
 *
 * This actor is also involved in disaster recovery and implements a state machine with the following
 * possible transitions:
 *
 *  - `initializing` -> `recovering` -> `processing` (when calling `endpoint.recover()`)
 *  - `initializing` -> `processing`                 (when calling `endpoint.activate()`)
 */
private class Acceptor(endpoint: ReplicationEndpoint) extends Actor {

  import Acceptor._
  import context.dispatcher

  private val recovery = new Recovery(endpoint)

  def initializing: Receive = recovering orElse {
    case Process =>
      context.become(processing)
  }

  def recovering: Receive = {
    case Recover(links, promise) =>
      val recoveryManager = context.actorOf(Props(new RecoveryManager(endpoint.id, links)))
      context.become(recoveringEvents(recoveryManager, promise) orElse processing)
    case RecoverCompleted =>
      context.become(processing)
  }

  def recoveringEvents(recoveryManager: ActorRef, promise: Promise[Unit]): Receive = {
    case writeSuccess: ReplicationWriteSuccess =>
      recoveryManager forward writeSuccess
    case RecoverEventCompleted =>
      promise.success(())
      context.become(recovering orElse processing)
  }

  def processing: Receive = {
    case re: ReplicationReadEnvelope if re.incompatibleWith(endpoint.applicationName, endpoint.applicationVersion) =>
      sender ! ReplicationReadFailure(IncompatibleApplicationVersionException(endpoint.id, endpoint.applicationVersion, re.targetApplicationVersion), re.payload.targetLogId)
    case ReplicationReadEnvelope(r, logName, _, _) =>
      val r2 = r.copy(filter = endpoint.endpointFilters.filterFor(r.targetLogId, logName) and r.filter)
      endpoint.logs(logName) forward r2
    case _: ReplicationWriteSuccess =>
  }

  override def unhandled(message: Any): Unit = message match {
    case GetReplicationEndpointInfo =>
      recovery.readEndpointInfo.map(GetReplicationEndpointInfoSuccess).pipeTo(sender())
    case SynchronizeReplicationProgress(remoteInfo) =>
      val localInfo = for {
        _ <- recovery.synchronizeReplicationProgress(remoteInfo)
        localInfo <- recovery.readEndpointInfo.map(SynchronizeReplicationProgressSuccess)
      } yield localInfo
      localInfo.recover {
        case ex: Throwable => SynchronizeReplicationProgressFailure(SynchronizeReplicationProgressSourceException(ex.getMessage))
      } pipeTo sender()
    case _ =>
      super.unhandled(message)
  }

  def receive =
    initializing
}

private object Acceptor {
  val Name = "replication-acceptor"

  def props(endpoint: ReplicationEndpoint): Props =
    Props(classOf[Acceptor], endpoint).withDispatcher(endpoint.settings.acceptorDispatcher)

  case object Process
  case class Recover(links: Set[RecoveryLink], promise: Promise[Unit])
  case object RecoverCompleted
  case class RecoverStepCompleted(link: RecoveryLink)
  case object RecoverMetadataCompleted
  case object RecoverEventCompleted

}

/**
 * If disaster recovery is initiated events are recovered until
 * a [[ReplicationWriteSuccess]] sent as notification from the local [[Replicator]] is received indicating that all
 * events, known to exist remotely at the beginning of recovery, are replicated.
 *
 * When all replication links have been processed this actor
 * notifies [[Acceptor]] (= parent) that recovery completed and ends itself.
 */
private class RecoveryManager(endpointId: String, links: Set[RecoveryLink]) extends Actor with ActorLogging {

  import Acceptor._

  def receive = recoveringEvents(links)

  private def recoveringEvents(active: Set[RecoveryLink]): Receive = {
    case writeSuccess: ReplicationWriteSuccess if active.exists(link => writeSuccess.metadata.contains(link.replicationLink.source.logId)) =>
      active.find(recoveryForLinkFinished(_, writeSuccess)).foreach { link =>
        val updatedActive = removeLink(active, link)
        if (updatedActive.isEmpty) {
          context.parent ! RecoverEventCompleted
          self ! PoisonPill
        } else
          context.become(recoveringEvents(updatedActive))
      }
  }

  private def recoveryForLinkFinished(link: RecoveryLink, writeSuccess: ReplicationWriteSuccess): Boolean =
    writeSuccess.metadata.get(link.replicationLink.source.logId) match {
      case Some(md) => link.remoteSequenceNr <= md.replicationProgress
      case None     => false
    }

  private def removeLink(active: Set[RecoveryLink], link: RecoveryLink): Set[RecoveryLink] = {
    val updatedActive = active - link
    val finished = links.size - updatedActive.size
    val all = links.size
    log.info("[recovery of {}] Event recovery finished for remote log {} ({} of {})", endpointId, link.replicationLink.source.logId, finished, all)
    updatedActive
  }
}

private object Controller {
  val Name = "replication-controller"

  def props(endpoint: ReplicationEndpoint) =
    Props(classOf[Controller], endpoint).withDispatcher(endpoint.settings.controllerDispatcher)

  case object GetReplicationConnections
  case class GetReplicationConnectionsSuccess(connections: Set[ReplicationConnection])
  case class ReplicationConnectionReachable(conn: ReplicationConnection)
  case class ReplicationConnectionUnreachable(conn: ReplicationConnection)
  case class ReachableReplicationConnection(connection: ReplicationConnection)
  case class UnreachableReplicationConnection(connection: ReplicationConnection)

  case class ActivateReplication(link: ReplicationLink)
  case class DeactivateReplication(link: ReplicationLink)
}

private class Controller(endpoint: ReplicationEndpoint) extends Actor with ActorLogging {

  import Controller._

  var replicatorRegistry = ReplicatorRegistry()
  val replicationDetector = context.actorOf(
    ReplicationDetector.props(endpoint.connections, endpoint.connectionRoles).withDispatcher(endpoint.settings.controllerDispatcher),
    ReplicationDetector.Name
  )

  override def receive: Receive = {

    case GetReplicationConnections =>
      replicationDetector forward GetReplicationConnections

    case ActivateReplication(link) =>
      log.warning("activate replication link with {}", link)
      replicatorRegistry(link).foreach { replicator =>
        replicatorRegistry = replicatorRegistry - link
        context stop replicator
      }

      val replicator = context.actorOf(
        Props(new Replicator(link.target, link.source)).withDispatcher(endpoint.settings.controllerDispatcher)
      )
      context.watch(replicator)
      replicatorRegistry = replicatorRegistry + (link, replicator)

    case DeactivateReplication(link) =>
      log.warning("deactivate replication link with {}", link)
      replicatorRegistry(link).foreach { replicator =>
        replicatorRegistry = replicatorRegistry - link
        context stop replicator
      }

    case ReachableReplicationConnection(connection) =>
      context.actorOf(
        Props(new ReplicatorInitializer(endpoint, connection)).withDispatcher(endpoint.settings.controllerDispatcher)
      )

    case UnreachableReplicationConnection(connection) =>
      replicatorRegistry.relicators.keys filter { link =>
        endpoint.replicationAcceptor(connection) == link.source.acceptor
      } foreach { link =>
        self ! DeactivateReplication(link)
      }

    case ReplicationConnectionReachable(conn) =>
      log.warning(
        "replication connection[{}@{}:{}] reachable, activate it.", conn.name, conn.host, conn.port
      )
      self ! ReachableReplicationConnection(conn)

    case ReplicationConnectionUnreachable(conn) =>
      log.warning(
        "replication connection[{}@{}:{}] unreachable, deactivate it.", conn.name, conn.host, conn.port
      )

      self ! UnreachableReplicationConnection(conn)

    case Terminated(actor) =>
      log.warning("replicator[path={}] terminated.", actor.path)
  }
}

private object ReplicationDetector {
  val Name = "replication-detector"

  def props(connections: Set[ReplicationConnection], connectionRoles: Set[String]): Props =
    if (connections.nonEmpty) {
      Props(classOf[DirectDetector], connections)
    } else if (connectionRoles.nonEmpty) {
      Props(classOf[ClusterDetector], connectionRoles)
    } else throw new IllegalArgumentException("eventuate.endpoint.connections and eventuate.endpoint.connection-roles both empty.")

  private class DirectDetector(conns: Set[ReplicationConnection]) extends Actor {

    import Controller._

    override def receive: Receive = {
      case GetReplicationConnections =>
        sender() ! GetReplicationConnectionsSuccess(conns)
    }
  }

  private case object ClusterInitiated
  private class ClusterDetector(roles: Set[String]) extends Actor with ActorLogging with Stash {

    import Controller._

    val cluster = Cluster(context.system)
    var connectionRegistry = ConnectionRegistry()

    @scala.throws[Exception](classOf[Exception])
    override def preStart(): Unit = {
      cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberUp], classOf[ReachableMember], classOf[UnreachableMember])
      cluster.registerOnMemberUp(self ! ClusterInitiated)
      context become initiating
    }

    override def receive: Receive = initiating

    private def initiating: Receive = {
      case MemberUp(member) =>
        if (avaliableMember(member)) {
          avaliableConnection(member).foreach { conn =>
            connectionRegistry = connectionRegistry + conn
          }
        }

      case ClusterInitiated =>
        context become initiated
        unstashAll()
        log.warning("cluster size reached, network initiated.")

      case GetReplicationConnections =>
        stash()
      case _ =>
    }

    private def initiated: Receive = {
      case GetReplicationConnections =>
        sender ! GetReplicationConnectionsSuccess(connectionRegistry.connections)
      case MemberUp(member) =>
        if (avaliableMember(member)) {
          avaliableConnection(member).foreach { conn =>
            connectionRegistry = connectionRegistry + conn
            context.parent ! ReplicationConnectionReachable(conn)
          }
        }

      case ReachableMember(member) =>
        if (avaliableMember(member)) {
          avaliableConnection(member).foreach { conn =>
            connectionRegistry = connectionRegistry + conn
            context.parent ! ReplicationConnectionReachable(conn)
          }
        }

      case UnreachableMember(member) =>
        if (avaliableMember(member)) {
          avaliableConnection(member).foreach { conn =>
            connectionRegistry = connectionRegistry - conn
            context.parent ! ReplicationConnectionUnreachable(conn)
          }
        }
      case _ =>
    }

    private def avaliableMember(member: Member): Boolean = {
      cluster.selfUniqueAddress != member.uniqueAddress && roles.intersect(member.roles).nonEmpty
    }

    private def avaliableConnection(member: Member): Option[ReplicationConnection] = for {
      host <- member.address.host
      port <- member.address.port
      system = member.address.system
    } yield ReplicationConnection(host, port, name = system)

    override def postStop(): Unit = cluster.unsubscribe(self)
  }

  private case class ConnectionRegistry(connections: Set[ReplicationConnection] = Set.empty) {
    def +(connection: ReplicationConnection): ConnectionRegistry =
      copy(connections = connections + connection)

    def -(connection: ReplicationConnection): ConnectionRegistry =
      copy(connections = connections - connection)
  }
}

/**
 * Replicates events from a remote source log to a local target log. This replicator guarantees that
 * the ordering of replicated events is preserved. Potential duplicates are either detected at source
 * (which is an optimization) or at target (for correctness). Duplicate detection is based on tracked
 * event vector times.
 */
private class Replicator(target: ReplicationTarget, source: ReplicationSource) extends Actor with ActorLogging {

  import FailureDetector._
  import context.dispatcher
  import target.endpoint.settings

  val scheduler = context.system.scheduler
  val detector = context.actorOf(Props(new FailureDetector(source.endpointId, source.logName, settings.failureDetectionLimit)))

  var readSchedule: Option[Cancellable] = None

  val fetching: Receive = {
    case GetReplicationProgressSuccess(_, storedReplicationProgress, currentTargetVersionVector) =>
      context.become(reading)
      read(storedReplicationProgress, currentTargetVersionVector)
    case GetReplicationProgressFailure(cause) =>
      log.warning("replication progress read failed: {}", cause)
      scheduleFetch()
  }

  val idle: Receive = {
    case ReplicationDue =>
      readSchedule.foreach(_.cancel()) // if it's notification from source concurrent to a scheduled read
      context.become(fetching)
      fetch()
  }

  val reading: Receive = {
    case ReplicationReadSuccess(events, fromSequenceNr, replicationProgress, _, currentSourceVersionVector) =>
      detector ! AvailabilityDetected
      context.become(writing)
      write(events, replicationProgress, currentSourceVersionVector, replicationProgress >= fromSequenceNr)
    case ReplicationReadFailure(cause, _) =>
      detector ! FailureDetected(cause)
      log.warning(s"replication read failed: {}", cause)
      context.become(idle)
      scheduleRead()
  }

  val writing: Receive = {
    case writeSuccess @ ReplicationWriteSuccess(_, _, false) =>
      notifyLocalAcceptor(writeSuccess)
      context.become(idle)
      scheduleRead()
    case writeSuccess @ ReplicationWriteSuccess(_, metadata, true) =>
      val sourceMetadata = metadata(source.logId)
      notifyLocalAcceptor(writeSuccess)
      context.become(reading)
      read(sourceMetadata.replicationProgress, sourceMetadata.currentVersionVector)
    case ReplicationWriteFailure(cause) =>
      log.warning("replication write failed: {}", cause)
      context.become(idle)
      scheduleRead()
  }

  def receive = fetching

  override def unhandled(message: Any): Unit = message match {
    case ReplicationDue => // currently replicating, ignore
    case other          => super.unhandled(message)
  }

  private def notifyLocalAcceptor(writeSuccess: ReplicationWriteSuccess): Unit =
    target.endpoint.acceptor ! writeSuccess

  private def scheduleFetch(): Unit =
    scheduler.scheduleOnce(settings.retryDelay)(fetch())

  private def scheduleRead(): Unit =
    readSchedule = Some(scheduler.scheduleOnce(settings.retryDelay, self, ReplicationDue))

  private def fetch(): Unit = {
    implicit val timeout = Timeout(settings.readTimeout)

    target.log ? GetReplicationProgress(source.logId) recover {
      case t => GetReplicationProgressFailure(t)
    } pipeTo self
  }

  private def read(storedReplicationProgress: Long, currentTargetVersionVector: VectorTime): Unit = {
    implicit val timeout = Timeout(settings.remoteReadTimeout)
    val replicationRead = ReplicationRead(storedReplicationProgress + 1, settings.writeBatchSize, settings.remoteScanLimit, NoFilter, target.logId, self, currentTargetVersionVector)

    (source.acceptor ? ReplicationReadEnvelope(replicationRead, source.logName, target.endpoint.applicationName, target.endpoint.applicationVersion)) recover {
      case t => ReplicationReadFailure(ReplicationReadTimeoutException(settings.remoteReadTimeout), target.logId)
    } pipeTo self
  }

  private def write(events: Seq[DurableEvent], replicationProgress: Long, currentSourceVersionVector: VectorTime, continueReplication: Boolean): Unit = {
    implicit val timeout = Timeout(settings.writeTimeout)

    target.log ? ReplicationWrite(events, Map(source.logId -> ReplicationMetadata(replicationProgress, currentSourceVersionVector)), continueReplication) recover {
      case t => ReplicationWriteFailure(t)
    } pipeTo self
  }

  override def preStart(): Unit =
    fetch()

  override def postStop(): Unit =
    readSchedule.foreach(_.cancel())
}

private case class ReplicatorRegistry(relicators: Map[ReplicationLink, ActorRef] = Map.empty) {

  def +(link: ReplicationLink, replicator: ActorRef): ReplicatorRegistry = {
    copy(relicators = relicators + (link -> replicator))
  }

  def -(link: ReplicationLink): ReplicatorRegistry = {
    copy(relicators = relicators - link)
  }

  def apply(link: ReplicationLink): Option[ActorRef] = {
    relicators.get(link)
  }
}

/**
 * If `replicationLinks` is [[None]] reliably sends [[GetReplicationEndpointInfo]] requests to the [[Acceptor]] at a source [[ReplicationEndpoint]].
 * On receiving a [[GetReplicationEndpointInfoSuccess]] reply, this connector sets up log [[Replicator]]s, one per
 * common log name between source and target endpoints.
 *
 * If `replicationLinks` is not [[None]] [[Replicator]]s will be setup for the given [[ReplicationLink]]s.
 */
private class ReplicatorInitializer(
  endpoint: ReplicationEndpoint,
  connection: ReplicationConnection) extends Actor {

  import Controller._
  import context.dispatcher

  private val acceptor = endpoint.replicationAcceptor(connection)
  private var acceptorRequestSchedule: Option[Cancellable] = None

  def receive = {
    case GetReplicationEndpointInfoSuccess(info) =>
      acceptorRequestSchedule.foreach(_.cancel())
      endpoint.replicationLinks(connection, info).foreach { link =>
        context.parent ! ActivateReplication(link)
      }
      context stop self
  }

  private def scheduleAcceptorRequest(acceptor: ActorSelection): Cancellable =
    context.system.scheduler.schedule(0.seconds, endpoint.settings.retryDelay, new Runnable {
      override def run() = acceptor ! GetReplicationEndpointInfo
    })

  override def preStart(): Unit =
    acceptorRequestSchedule = Some(scheduleAcceptorRequest(acceptor))

  override def postStop(): Unit =
    acceptorRequestSchedule.foreach(_.cancel())
}

private object FailureDetector {
  case object AvailabilityDetected
  case class FailureDetected(cause: Throwable)
  case class FailureDetectionLimitReached(counter: Int)
}

private class FailureDetector(sourceEndpointId: String, logName: String, failureDetectionLimit: FiniteDuration) extends Actor with ActorLogging {

  import FailureDetector._
  import ReplicationEndpoint._
  import context.dispatcher

  private var counter: Int = 0
  private var causes: Vector[Throwable] = Vector.empty
  private var schedule: Cancellable = scheduleFailureDetectionLimitReached()

  private val failureDetectionLimitNanos = failureDetectionLimit.toNanos
  private var lastReportedAvailability: Long = 0L

  def receive = {
    case AvailabilityDetected =>
      val currentTime = System.nanoTime()
      val lastInterval = currentTime - lastReportedAvailability
      if (lastInterval >= failureDetectionLimitNanos) {
        context.system.eventStream.publish(Available(sourceEndpointId, logName))
        lastReportedAvailability = currentTime
      }
      schedule.cancel()
      schedule = scheduleFailureDetectionLimitReached()
      causes = Vector.empty
    case FailureDetected(cause) =>
      causes = causes :+ cause
    case FailureDetectionLimitReached(scheduledCount) if scheduledCount == counter =>
      log.error(causes.lastOption.getOrElse(Logging.Error.NoCause), "replication failure detection limit reached ({})," +
        " publishing Unavailable for {}/{} (last exception being reported)", failureDetectionLimit, sourceEndpointId, logName)
      context.system.eventStream.publish(Unavailable(sourceEndpointId, logName, causes))
      schedule = scheduleFailureDetectionLimitReached()
      causes = Vector.empty
  }

  private def scheduleFailureDetectionLimitReached(): Cancellable = {
    counter += 1
    context.system.scheduler.scheduleOnce(failureDetectionLimit, self, FailureDetectionLimitReached(counter))
  }
}

private class Replication(endpoint: ReplicationEndpoint) {
  import Acceptor._
  import Controller._

  def awaitReplicationConnections(implicit ec: ExecutionContext): Future[Set[ReplicationConnection]] = {
    implicit val timeout = Timeout(endpoint.settings.activeTimeout)
    (endpoint.controller ? GetReplicationConnections).asInstanceOf[Future[GetReplicationConnectionsSuccess]] map {
      _.connections
    }
  }

  def activateReplicationConnections(connections: Set[ReplicationConnection]): Unit = {
    endpoint.acceptor ! Process
    for (conn <- connections) {
      endpoint.controller ! ReachableReplicationConnection(conn)
    }
  }
}

/**
 * References a remote event log at a source [[ReplicationEndpoint]].
 */
private case class ReplicationSource(
  endpointId: String,
  logName: String,
  logId: String,
  acceptor: ActorSelection
)

/**
 * References a local event log at a target [[ReplicationEndpoint]].
 */
private case class ReplicationTarget(
  endpoint: ReplicationEndpoint,
  logName: String,
  logId: String,
  log: ActorRef
) {
}

/**
 * Represents an unidirectional replication link between a `source` and a `target`.
 */
private case class ReplicationLink(source: ReplicationSource, target: ReplicationTarget) {

  override def equals(other: scala.Any): Boolean = other match {
    case that: ReplicationLink =>
      (that canEqual this) &&
        source == that.source &&
        target == that.target
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(source, target)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}