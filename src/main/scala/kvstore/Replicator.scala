package kvstore

import akka.actor.{Actor, ActorRef, Cancellable, Props}

import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)

  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)

  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate, Cancellable)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case r@Replicate(key, valueOption, id) =>
      println(s"received message Replicate:${r} from ${sender()}")
      val seq = nextSeq()
      val cancellable = context.system.scheduler.scheduleWithFixedDelay(0.millis, 100.millis)(() => replica ! Snapshot(key, valueOption, seq))
      acks = acks.updated(seq, (sender(), r, cancellable))
    case s@SnapshotAck(key, seq) =>
      println(s"recieve SnapshotAck:${s}")
      acks.get(seq).foreach { p =>
        p._3.cancel()
        p._1 ! Replicated(p._2.key, p._2.id)
      }
      acks = acks.removed(seq)
  }

}
