package ericj.chelan.raft

import akka.testkit.{ DefaultTimeout, TestKit, TestFSMRef, TestProbe }
import ericj.chelan.raft.messages.{ AppendEntriesRequest, RequestVoteResponse, RequestVoteRequest, Init }
import akka.actor.FSM.StateTimeout
import akka.actor.ActorSystem
import org.scalatest._
import CustomMatchers._

/**
 *
 * Created by Eric Jutrzenka on 13/03/2014.
 */
class CandidateSpec extends RaftSpec {

  "A Candidate" should "send out vote requests when it starts a new term." in {
    f =>
      forAll(f.probes) {
        p =>
          p.expectMsg(RequestVoteRequest(f.initialTerm))
      }
  }
  it should "start a new term when it times out as a candidate" in {
    f =>
      f.server ! StateTimeout
      f.server should beInState(Candidate)
      f.server should beInTerm(f.initialTerm + 1)
      forAll(f.probes) {
        p =>
          p.expectMsgType[RequestVoteRequest]
      }
  }
  it should "not repeat a RequestVoteReques when a vote has been counted" in {
    f => pending
  }
  it should "stand down on RequestVoteRequest with greater term" in {
    f =>
      val initialTerm = f.server.stateData.currentTerm
      f.server ! RequestVoteRequest(initialTerm + 1)
      f.server should beInState(Follower)
  }
  it should "stand down on AppendEntriesRequest with greater term" in {
    f =>
      val initialTerm = f.server.stateData.currentTerm
      f.server ! AppendEntriesRequest(initialTerm + 2)
      f.server should beInState(Follower)
      f.server should beInTerm(initialTerm + 2)
  }
  it should "stand down on AppendEntriesRequest with the same term" in {
    f =>
      val initialTerm = f.server.stateData.currentTerm
      f.server ! AppendEntriesRequest(initialTerm)
      f.server should beInState(Follower)
  }
  it should "be elected leader when it receives enough votes" in {
    f =>
      f.probes(0).expectMsg(RequestVoteRequest(f.initialTerm))
      f.probes(0).reply(RequestVoteResponse(f.initialTerm, success = true))
      f.server shouldNot beInState(Leader)
      f.probes(1).expectMsg(RequestVoteRequest(f.initialTerm))
      f.probes(1).reply(RequestVoteResponse(f.initialTerm, success = true))
      f.server should beInState(Leader)
  }
  it should "ignore votes from the same elector" in {
    f =>
      f.probes(0).expectMsg(RequestVoteRequest(f.initialTerm))
      f.probes(0).reply(RequestVoteResponse(f.initialTerm, success = true))
      f.server shouldNot beInState(Leader)
      f.probes(0).expectMsg(RequestVoteRequest(f.initialTerm))
      f.probes(0).reply(RequestVoteResponse(f.initialTerm, success = true))
      f.server.stateName should not be Leader
  }
  it should "not count votes that are not granted" in {
    f =>
      f.probes(0).expectMsg(RequestVoteRequest(f.initialTerm))
      f.probes(0).reply(RequestVoteResponse(f.initialTerm, success = true))
      f.server shouldNot beInState(Leader)
      f.probes(1).expectMsg(RequestVoteRequest(f.initialTerm))
      f.probes(1).reply(RequestVoteResponse(f.initialTerm, success = false))
      f.server shouldNot beInState(Leader)
  }
  it should "queue client requests until a leader is elected" in {
    f => pending
  }

  override def init(f: FixtureParam): FixtureParam = {
    f.becomeCandidate()
    f
  }

}

