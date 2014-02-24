package ericj.chelan.raft.fsm

import akka.testkit.{TestActorRef, TestFSMRef}
import akka.actor.{Actor, ActorRef, FSM}
import scala.concurrent.duration._
import ericj.chelan.UnitSpec
import ericj.chelan.raft.fsm.NotStarted
import akka.actor.FSM.StateTimeout

import org.scalatest._
import Matchers._

/**
 * Created by ericj on 23/02/2014.
 */
class RaftFsmActorSpec extends UnitSpec {

  "A server" should "start in the NotStarted state" in {
    val fsm = TestFSMRef(new RaftFsmActor)
    assert(fsm.stateName == NotStarted)
    assert(fsm.stateData == Uninitialized)
  }
  it should "transition to Follower when initialised" in {
    val fsm = TestFSMRef(new RaftFsmActor)
    fsm ! Init(Array.empty)
    assert(fsm.stateName == Follower)
  }
  
  "A Follower" should "send out VoteRequests to all members when it transitions to Candidate" in {
    var member1Term: Int = -1
    var member2Term: Int = -1
    val initialTerm: Int = 0

    val member1 = TestActorRef(new Actor {
      override def receive: Actor.Receive = {
        case RequestVote(term) => member1Term = term
      }
    })
    val member2 = TestActorRef(new Actor {
      override def receive: Actor.Receive = {
        case RequestVote(term) => member2Term = term
      }
    })
    val fsm = TestFSMRef(new RaftFsmActor)
    fsm.setState(Follower, Generic(initialTerm, Array(member1, member2), ActorRef.noSender))
    fsm ! StateTimeout
    assert(member1Term == initialTerm + 1)
    assert(member2Term == initialTerm + 1)
  }
  it should "transition to candidate after the election timeout" in {
    val fsm = TestFSMRef(new RaftFsmActor)
    fsm.setState(Follower, Generic(1, Array.empty, ActorRef.noSender))
    fsm ! StateTimeout
    assert(fsm.stateName == Candidate)
  }

  "A Candidate" should "transition to Follower when term > currentTerm" in {
    val fsm = TestFSMRef(new RaftFsmActor)
    fsm.setState(Candidate, Generic(0, Array.empty, ActorRef.noSender))
    fsm ! AppendEntriesRpc(1)
  }


}
