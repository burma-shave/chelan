package ericj.chelan

import akka.testkit.{DefaultTimeout, TestKitBase, TestKit, TestActorRef}
import akka.pattern.ask
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.actor.ActorSystem
import org.scalatest._
import scala.util.Success


/**
 * Created by ericj on 13/02/2014.
 */
class KvsActorSpec extends UnitSpec {

  import KeyImplicits._

  "A Kvs actor" should "return Success(None) for non-existent value" in {
    val actorRef = TestActorRef(new Kvs)
    val future = actorRef ? Get(1)
    future.value.get should be (Success(None))
  }
  it should "store a value" in {
    val actorRef = TestActorRef(new Kvs)
    actorRef ! Put(1, "foo")
  }
  it should "return Success(Some(value)) for an existent value" in {
    val actorRef = TestActorRef(new Kvs)
    actorRef ! Put(1, "expectedValue")
    val future = actorRef ? Get(1)
    future.value.get should be (Success(Some("expectedValue")))
  }
  it should "overwrite values with the same key" in {
    val actorRef = TestActorRef(new Kvs)
    actorRef ! Put(1, "oldValue")
    actorRef ! Put(1, "newValue")
    val future = actorRef ? Get(1)
    future.value.get should be (Success(Some("newValue")))
  }
  it should "not return a value if it has been deleted" in {
    val actorRef = TestActorRef(new Kvs)
    actorRef ! Put(1, "oldValue")
    actorRef ! Remove(1)
    val future = actorRef ? Get(1)
    future.value.get should be (Success(None))
  }
}
