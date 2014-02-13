package ericj.chelan

import akka.actor.{Props, Actor}
import akka.pattern.ask
import scala.concurrent.{Await, Future}
import akka.util.Timeout
import scala.concurrent.duration._
import scala.util.Try


/**
 * Hello world actor.
 *
 * Created by ericj on 13/02/2014.
 */
class HelloWorld extends Actor {

  import KeyImplicits._

  override def preStart(): Unit = {
    implicit val timeout = Timeout(5 seconds)

    val kvs = context.actorOf(Props[Kvs], "kvs")
    kvs ! Put(1, "foo")
    val future: Future[Option[String]] = (kvs ? Get(1)).mapTo[Option[String]]
  }

  def receive: Actor.Receive = {
    case Some(s) => println(s"value: $s")
    case None => println("No value")
  }
}
