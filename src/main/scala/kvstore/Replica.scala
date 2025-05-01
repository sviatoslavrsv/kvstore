package kvstore

import akka.Done
import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, Cancellable, Kill, OneForOneStrategy, Props, SupervisorStrategy, Terminated}
import akka.util.Timeout
import kvstore.Arbiter._
import kvstore.Replicator.{Replicate, Replicated, Snapshot, SnapshotAck}

import scala.concurrent.duration._

object Replica {
  sealed trait Operation {
    def key: String

    def id: Long
  }

  case class Insert(key: String, value: String, id: Long) extends Operation

  case class Remove(key: String, id: Long) extends Operation

  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply

  case class OperationAck(id: Long) extends OperationReply

  case class OperationFailed(id: Long) extends OperationReply

  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

  import Persistence._
  import Replica._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  implicit val timeout: Timeout = Timeout(100.millis)
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  var persistence: ActorRef = context.system.actorOf(persistenceProps)
  var persistAcks = Map.empty[Long, (ActorRef, Cancellable)]
  var replicatedAcksInsert = Map.empty[ActorRef, Set[Long]]
  var replicatedAcksRemove = Map.empty[ActorRef, Set[Long]]
  arbiter ! Join

  def receive: Receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica(0L))
  }

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(withinTimeRange = 1.seconds) {
    case _: PersistenceException => Restart
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) =>
      val send = sender()
      kv = kv.updated(key, value)
      replicators.foreach { repl =>
        replicatedAcksInsert = replicatedAcksInsert.updated(repl, replicatedAcksInsert.get(repl).toSet.flatten + id)
        repl ! Replicate(key, Some(value), id)
      }
      //add primary replica
      replicatedAcksInsert = replicatedAcksInsert.updated(self, replicatedAcksInsert.get(self).toSet.flatten + id)
      persist(key, Some(value), id, sender())
      context.system.scheduler.scheduleOnce(1000.millis) {
        if (replicators.exists(repl => replicatedAcksInsert.get(repl).exists(_.contains(id)))) {
          send ! OperationFailed(id)
        } else {
          send ! OperationAck(id)
        }
      }
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)
    case Remove(key, id) =>
      val send = sender()
      kv = kv.removed(key)
      replicators.foreach { repl =>
        replicatedAcksInsert = replicatedAcksInsert.updated(repl, replicatedAcksInsert.get(repl).toSet.flatten + id)
        repl ! Replicate(key, None, id)
      }
      //add primary replica
      replicatedAcksInsert = replicatedAcksInsert.updated(self, replicatedAcksInsert.get(self).toSet.flatten + id)
      persist(key, None, id, sender())
      context.system.scheduler.scheduleOnce(1000.millis) {
        if (replicators.exists(repl => replicatedAcksInsert.get(repl).exists(_.contains(id)))) {
          send ! OperationFailed(id)
        } else {
          send ! OperationAck(id)
        }
      }
    case Replicas(allReplicas) =>
      println(allReplicas)
      //remove old
      val torem = secondaries.values.filter(act => !allReplicas.contains(act))
      torem.foreach { actToRem =>
        println(s"Primary to remove old:${actToRem}")
        replicatedAcksInsert = replicatedAcksInsert - actToRem
        secondaries.get(actToRem).foreach { replicator =>
          replicators = replicators - replicator
          replicator ! Kill
        }
        secondaries = secondaries.removed(actToRem)
        actToRem ! Kill
      } //add new
      val toadd = allReplicas.diff(secondaries.values.toSet).filter(_ != self)
      toadd.foreach { replica =>
        println(s"Primary new replica:${replica}")
        context.watch(replica)
        val replicator = context.actorOf(Replicator.props(replica), s"replicator_sec_${replica.hashCode()}")
        println(s"Primary new replicator:${replicator}")
        replicators = replicators + replicator
        secondaries = secondaries.updated(replica, replicator)
        kv.foreach { case (key, value) => replicator ! Replicate(key, Some(value), 0) }
      }

    case Replicated(key, id) =>
      println(s"Primary Replicated ack for key:$key id:$id")
      replicatedAcksInsert = replicatedAcksInsert.updatedWith(sender())(_.map(_ - id))
    case p@Persisted(_, id) =>
      println(s"Primary Persisted:${p}")
      replicatedAcksInsert = replicatedAcksInsert.updatedWith(self)(_.map(_ - id))
      persistAcks.get(id).foreach {
        case (_, cancellable) =>
          cancellable.cancel()
          persistAcks = persistAcks.removed(id)
      }
    case t@Terminated(actorReplica) =>
      println(s"Primary actor was Terminated:${t.actor}")
      secondaries.get(actorReplica).map { replicator =>
        secondaries = secondaries.removed(actorReplica)
        replicators = replicators - replicator
      }
  }

  /* TODO Behavior for the replica role. */
  def replica(localSeq: Long): Receive = {
    case s@Snapshot(key, valueOption, seq) =>
      println(s"Snapshot secondary:${s}")
      if (seq == localSeq) {
        valueOption match {
          case Some(value) => kv = kv.updated(key, value)
          case None => kv = kv.removed(key)
        }
        context.become(replica(localSeq + 1))
        persist(key, valueOption, seq, sender())
      }
      if (seq < localSeq) sender() ! SnapshotAck(key, seq)
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)

    case p@Persisted(key, id) =>
      println(s"Secondary Persisted:${p}")
      persistAcks.get(id).foreach {
        case (sendor, cancellable) =>
          cancellable.cancel()
          sendor ! SnapshotAck(key, id)
          persistAcks = persistAcks.removed(id)
      }
  }

  private def persist(key: String, valueOption: Option[String], id: Long, sender: ActorRef): Unit = {
    val send = sender
    val pers = persistence
    val cancellable = context.system.scheduler.scheduleAtFixedRate(0.millis, 100.millis, pers, Persist(key, valueOption, id))
    persistAcks = persistAcks.updated(id, (send, cancellable))
  }
}

