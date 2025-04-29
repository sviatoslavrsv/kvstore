package kvstore

import akka.actor.{Actor, ActorRef, Props, Terminated}
import akka.pattern.ask
import akka.util.Timeout
import kvstore.Arbiter._

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.util.{Failure, Success}

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
  implicit val timeout: Timeout = Timeout(3.seconds)
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  var persistence: ActorRef = context.system.actorOf(persistenceProps)
  arbiter ! Join

  def receive: Receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) =>
      kv = kv.updated(key, value)
      //      secondaries.foreach()
      persist(key, Some(value), id)
      sender() ! OperationAck(id)
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)
    case Remove(key, id) =>
      kv = kv.removed(key)
      persist(key, None, id)
      sender() ! OperationAck(id)
    case Replicas(allReplicas) =>
      val newReplica = allReplicas.diff(secondaries.values.toSet)
      newReplica.map { replica =>
        context.watch(replica)
        val rtor = context.actorOf(Replicator.props(replica))
        replicators = replicators + rtor
        secondaries = secondaries.updated(replica, rtor)
      }
    case Terminated =>
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)

  }

  private def persist(key: String, value: Option[String], id: Long): Persisted = {
    val f = repeat {
      val p = Promise[Persisted]()
      (persistence ? Persist(key, value, id))
        .onComplete {
          case Failure(exception) =>
            persistence = context.system.actorOf(persistenceProps)
            p.failure(exception)
          case Success(value) =>
            p.success(value.asInstanceOf[Persisted])
        }
      p.future
    }.until(f => f.value.isEmpty)

    Await.result(f, 2.seconds)
  }

  private def repeat[A](body: => A) = new {
    def until(condition: A => Boolean): A = {
      var a = body
      while (!condition(a)) {
        a = body
      }
      a
    }
  }
}

