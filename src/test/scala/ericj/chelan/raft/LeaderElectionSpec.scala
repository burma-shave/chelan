package ericj.chelan.raft

import ericj.chelan.UnitSpec
import akka.actor.{ActorRef, Props}
import ericj.chelan.raft.messages.{NewLeader, Init}
import akka.actor.ActorDSL._
import scala.concurrent.duration._


/**
 * Created by ericj on 04/03/2014.
 */
class LeaderElectionSpec extends UnitSpec {

  "Raft" should "elect a leader" in {

    val parent = actor(new Act {

      val raftActor1 = context.actorOf(Props[RaftActor], "r1")
      val raftActor2 = context.actorOf(Props[RaftActor], "r2")
      val raftActor3 = context.actorOf(Props[RaftActor], "r3")
      val raftActor4 = context.actorOf(Props[RaftActor], "r4")
      val raftActor5 = context.actorOf(Props[RaftActor], "r5")

      val raftActorRefs: Array[ActorRef] = Array(raftActor1, raftActor2, raftActor3, raftActor4, raftActor5)

      val processorRefs = raftActorRefs.map {
        ref => (context.actorOf(RaftProcessor.props(0, ref), s"p:${ref.path.name}"), ref)
      }

      raftActorRefs foreach {
        raftRef => raftRef ! Init(processorRefs filter {
          pair => pair._2 != raftRef
        }map { _._1 })
      }

      become {
        case m: NewLeader => testActor forward m
      }
    })

    expectMsgType[NewLeader](10 seconds)
  }

}
