package kvstore

import akka.actor.{Props, Actor}
import scala.util.Random

object Persistence {
  case class Persist(key: String, valueOption: Option[String], id: Long)
  case class Persisted(key: String, id: Long)

  class PersistenceException extends Exception("Persistence failure")

  def props(flaky: Boolean): Props = Props(classOf[Persistence], flaky)
}

class Persistence(flaky: Boolean) extends Actor {
  import Persistence._

  def receive: Receive = {
    case p@Persist(key, _, id) =>
//      println(s"receive ${p}")
      if (!flaky || Random.nextBoolean()) sender() ! Persisted(key, id)
//      if (Random.nextBoolean()) sender() ! Persisted(key, id)
//        sender() ! Persisted(key, id)
      else throw new PersistenceException
  }

}
