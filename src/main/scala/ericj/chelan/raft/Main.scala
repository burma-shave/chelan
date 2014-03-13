package ericj.chelan.raft

import akka.actor.{ActorRef, Props, ActorSystem}
import ericj.chelan.raft.messages.Init

/**
 * Created by Eric Jutrzenka on 05/03/2014.
 */
object Main {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("Raft")

    val raftActor1 = system.actorOf(Props[RaftActor], "r1")
    val raftActor2 = system.actorOf(Props[RaftActor], "r2")
    val raftActor3 = system.actorOf(Props[RaftActor], "r3")
    val raftActor4 = system.actorOf(Props[RaftActor], "r4")
    val raftActor5 = system.actorOf(Props[RaftActor], "r5")

    val raftActorRefs: Array[ActorRef] = Array(raftActor1, raftActor2, raftActor3, raftActor4, raftActor5)

    raftActorRefs foreach {
      ref =>
        ref ! Init(raftActorRefs filter (_ != ref))
    }


  }

}
