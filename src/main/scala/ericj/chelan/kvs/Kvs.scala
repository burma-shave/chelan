package ericj.chelan.kvs

import akka.actor.Actor
import scala.collection.mutable
import com.roundeights.hasher.Hash

case class Put(val key: Hash, val value: String)

case class Get(val key: Hash)

case class Remove(val key: Hash)

object Done

object NotFound

class Kvs extends Actor {

  val store = mutable.Map[Hash, String]()

  def receive: Actor.Receive = {
    case Put(k, v) =>
      store += Pair(k, v)
    case Get(k) =>
      sender ! store.get(k)
    case Remove(k) =>
      store -= k
  }
}