package ericj.chelan

import spray.routing.HttpService
import akka.pattern.ask
import akka.actor.{Actor, Props, ActorRef}
import KeyImplicits._
import akka.util.Timeout
import scala.concurrent.duration._

/**
 * Created by ericj on 13/02/2014.
 */
class KvsServiceActor extends Actor with KvsService {
  def actorRefFactory = context

  def receive = runRoute(kvsRoute)
}

trait KvsService extends HttpService {

  implicit def executionContext = actorRefFactory.dispatcher
  private val kvs: ActorRef = actorRefFactory.actorOf(Props[Kvs])
  implicit val timeout: Timeout  = 1 second

  val kvsRoute = {
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
