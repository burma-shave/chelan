package ericj.chelan.raft

import ericj.chelan.UnitSpec
import akka.testkit.TestFSMRef
import ericj.chelan.raft.messages.{Vote, VoteRequest, UpdateTerm, Init}
import akka.actor.FSM.StateTimeout

/**
 * Created by ericj on 03/03/2014.
 */
class RaftActorSpec extends UnitSpec {

  "A server" should "start in the NotStarted state" in {
    val fsm = TestFSMRef(new RaftActor)
    assert(fsm.stateName == NotStarted)
  }
  it should "transition to Follower when initialised" in {
    val fsm = TestFSMRef(new RaftActor)
    fsm ! Init(Array.empty)
    assert(fsm.stateName == Follower)
  }
  it should "update its term when it receives an UpdateTerm message" in {
    val expectedTerm: Int = 5
    val fsm = TestFSMRef(new RaftActor)
    fsm ! Init(Array.empty)
    fsm ! UpdateTerm(expectedTerm)
    assert(fsm.stateData.replicaVars.currentTerm == expectedTerm)
    assert(fsm.stateData.replicaVars.votedFor == None)
  }
  it should "ignore UpdateTerm if the term is not newer" in {
    val fsm = TestFSMRef(new RaftActor)
    fsm ! Init(Array.empty)
    fsm ! StateTimeout
    fsm ! Elected
    fsm ! UpdateTerm(fsm.stateData.currentTerm)
    assert(fsm.stateName == Leader)
  }
  it should "become a leader when it is elected" in {
    val fsm = TestFSMRef(new RaftActor)
    fsm ! Init(Array.empty)
    fsm ! StateTimeout
    fsm ! Elected
    assert(fsm.stateName == Leader)
  }
  it should "become a candidate when it times out as a follower" in {
    val fsm = TestFSMRef(new RaftActor)
    fsm ! Init(Array(testActor))
    val initialTerm = fsm.stateData.replicaVars.currentTerm
    fsm ! StateTimeout
    assert(fsm.stateName == Candidate)
    assert(fsm.stateData.replicaVars.currentTerm == initialTerm + 1)
    expectMsgType[VoteRequest]
  }
  it should "start a new term when it times out as a candidate" in {
    val fsm = TestFSMRef(new RaftActor)
    fsm ! Init(Array(testActor))
    fsm ! StateTimeout
    val initialTerm = fsm.stateData.replicaVars.currentTerm
    fsm ! StateTimeout
    assert(fsm.stateName == Candidate)
    assert(fsm.stateData.replicaVars.currentTerm == initialTerm + 1)
    expectMsgType[VoteRequest]
  }
  it should "become a leader when it receives and Elected message" in {
    val fsm = TestFSMRef(new RaftActor)
    fsm ! Init(Array.empty)
    fsm ! StateTimeout
    fsm ! Elected
  }
}
