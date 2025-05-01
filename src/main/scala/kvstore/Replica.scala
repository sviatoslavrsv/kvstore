package kvstore

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, Cancellable, OneForOneStrategy, Props, SupervisorStrategy}
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
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  var persistence = context.system.actorOf(persistenceProps)
  var persistAcks = Map.empty[Long, (ActorRef, Cancellable)]
  var replicatedAcks = Map.empty[ActorRef, Set[Long]]
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
        replicatedAcks = replicatedAcks.updated(repl, replicatedAcks.get(repl).toSet.flatten + id)
        repl ! Replicate(key, Some(value), id)
      }
      //add primary replica
      replicatedAcks = replicatedAcks.updated(self, replicatedAcks.get(self).toSet.flatten + id)
      persist(key, Some(value), id, sender())
      context.system.scheduler.scheduleOnce(1000.millis)(checkAllAcks(send, id))
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)
    case Remove(key, id) =>
      val send = sender()
      kv = kv.removed(key)
      replicators.foreach { repl =>
        replicatedAcks = replicatedAcks.updated(repl, replicatedAcks.get(repl).toSet.flatten + id)
        repl ! Replicate(key, None, id)
      }
      //add primary replica
      replicatedAcks = replicatedAcks.updated(self, replicatedAcks.get(self).toSet.flatten + id)
      persist(key, None, id, sender())
      context.system.scheduler.scheduleOnce(1000.millis)(checkAllAcks(send, id))
    case Replicas(allReplicas) =>
      //remove old
      val torem = secondaries.keys.filter(act => !allReplicas.contains(act))
      torem.foreach { actToRem =>
        replicatedAcks = replicatedAcks - actToRem
        secondaries.get(actToRem).foreach { replicator =>
          replicators = replicators - replicator
          context.stop(replicator)
        }
        secondaries = secondaries.removed(actToRem)
        context.stop(actToRem)
      } //add new
      val toadd = allReplicas.diff(secondaries.keys.toSet).filter(_ != self)
      toadd.foreach { replica =>
        val replicator = context.actorOf(Replicator.props(replica), s"replicator_sec_${replica.hashCode()}")
        replicators = replicators + replicator
        secondaries = secondaries.updated(replica, replicator)
        kv.foreach { case (key, value) => replicator ! Replicate(key, Some(value), 0) }
      }

    case Replicated(key, id) =>
      replicatedAcks = replicatedAcks.updatedWith(sender())(_.map(_ - id))
    case Persisted(_, id) =>
      replicatedAcks = replicatedAcks.updatedWith(self)(_.map(_ - id))
      persistAcks.get(id).foreach {
        case (_, cancellable) =>
          cancellable.cancel()
          persistAcks = persistAcks.removed(id)
      }
  }

  /* TODO Behavior for the replica role. */
  def replica(localSeq: Long): Receive = {
    case Snapshot(key, valueOption, seq) =>
      if (seq == localSeq) {
        valueOption match {
          case Some(value) => kv = kv.updated(key, value)
          case None => kv = kv.removed(key)
        }
        persist(key, valueOption, seq, sender())
        context.become(replica(localSeq + 1))
      }
      if (seq < localSeq) {
        sender() ! SnapshotAck(key, seq)
      } else if (seq > localSeq) {
      }
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)

    case Persisted(key, id) =>
      persistAcks.get(id).foreach {
        case (sendor, cancellable) =>
          cancellable.cancel()
          sendor ! SnapshotAck(key, id)
          persistAcks = persistAcks.removed(id)
      }
  }

  private def persist(key: String, valueOption: Option[String], id: Long, sender: ActorRef): Unit = {
    val cancellable = context.system.scheduler.scheduleAtFixedRate(0.millis, 100.millis, persistence, Persist(key, valueOption, id))
    persistAcks = persistAcks.updated(id, (sender, cancellable))
  }

  private def checkAllAcks(sender: ActorRef, id: Long): Unit = {
    val replicasFailedExist = replicators.exists(repl => replicatedAcks.get(repl).exists(_.contains(id)))
    val primFailExists = replicatedAcks.get(self).exists(_.contains(id))
    if (replicasFailedExist || primFailExists) {
      sender ! OperationFailed(id)
    } else {
      sender ! OperationAck(id)
    }
  }
}

