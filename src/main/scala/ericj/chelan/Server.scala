package ericj.chelan

import spray.routing.SimpleRoutingApp
import akka.actor._
import akka.pattern.ask
import KeyImplicits._
import scala.concurrent.duration._
import akka.util.Timeout
import scala.util.Try
import scala.concurrent.Future


/**
 * Created by ericj on 13/02/2014.
 */
object Server extends App with SimpleRoutingApp {

  implicit val system = ActorSystem()
  implicit val timeout: Timeout = 1 second
  import system._

  private val kvs: ActorRef = system.actorOf(Props[Kvs])

  startServer(interface = "localhost", port = 9000) {
    path(""".+""".r) { key =>
      get {
        complete {
          ask(kvs, Get(key)).mapTo[Option[String]]
        }
      } ~
      put {
        entity(as[String]) { value =>
          complete {
            kvs ! Put(key, value)
            key
          }
        }
      } ~
      delete {
        complete {
          kvs ! Remove(key)
          "ok"
        }
      }
    }
  }

}
