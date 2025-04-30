package kvstore

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props, SupervisorStrategy, Terminated}
import kvstore.Arbiter._
import kvstore.Replicator.{Replicate, Snapshot, SnapshotAck}

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

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  var persistence: ActorRef = context.system.actorOf(persistenceProps)
  arbiter ! Join

  def receive: Receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica(0))
  }

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _: PersistenceException => Restart
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) =>
      kv = kv.updated(key, value)
      replicators.foreach(_ ! Replicate(key, Some(value), id))
      sender() ! OperationAck(id)
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)
    case Remove(key, id) =>
      kv = kv.removed(key)
      replicators.foreach(_ ! Replicate(key, None, id))
      sender() ! OperationAck(id)
    case Replicas(allReplicas) =>
      val newReplica = allReplicas.diff(secondaries.values.toSet)
      newReplica.map { replica =>
        context.watch(replica)
        val replicator = context.actorOf(Replicator.props(replica))
        replicators = replicators + replicator
        secondaries = secondaries.updated(replica, replicator)
      }
    case t@Terminated(actorReplica) =>
      println(s"actor was Terminated:${t.actor}")
      secondaries.get(actorReplica).map { replicator =>
        secondaries = secondaries.removed(actorReplica)
        replicators = replicators - replicator
      }
  }

  /* TODO Behavior for the replica role. */
  def replica(localSeq: Int): Receive = {
    case s@Snapshot(key, valueOption, seq) =>
      println(s"Snapshot:${s}, localSeq:$localSeq, kv:${kv.mkString(",")}")
      if (seq == localSeq) {
        valueOption match {
          case Some(value) =>
            kv = kv.updated(key, value)
          case None =>
            kv = kv.removed(key)
        }
        context.become(replica(localSeq + 1))
      }
      if (seq <= localSeq) {
        sender() ! SnapshotAck(key, seq)
      }
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)
  }

}

