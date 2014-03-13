package ericj.chelan.raft

import ericj.chelan.UnitSpec
import akka.actor.{ActorRef, Props}
import ericj.chelan.raft.messages.Init
import akka.actor.ActorDSL._
import akka.actor.FSM.{Transition, SubscribeTransitionCallBack}


/**
 * Created by Eric Jutrzenka on 04/03/2014.
 */
class LeaderElectionSpec extends UnitSpec {

  "Raft" should "elect a leader" in {
    val raftActor1 = system.actorOf(Props[RaftActor], "r1")
    val raftActor2 = system.actorOf(Props[RaftActor], "r2")
    val raftActor3 = system.actorOf(Props[RaftActor], "r3")
    val raftActor4 = system.actorOf(Props[RaftActor], "r4")
    val raftActor5 = system.actorOf(Props[RaftActor], "r5")

    val raftActorRefs: Array[ActorRef] = Array(raftActor1, raftActor2, raftActor3, raftActor4, raftActor5)

    raftActorRefs foreach {
      _ ! SubscribeTransitionCallBack(testActor)
    }

    raftActorRefs foreach {
      ref =>
        ref ! Init(raftActorRefs filter (_ != ref))
    }

    fishForMessage() {
      case Transition(_, Candidate, Leader) =>
        true
      case _ => false
    }

    raftActorRefs foreach {
      ref =>
        system.stop(ref)
    }
    system.shutdown()

  }

}
