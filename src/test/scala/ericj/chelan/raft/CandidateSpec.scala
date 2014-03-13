package ericj.chelan.raft

import akka.testkit.{DefaultTimeout, TestKit, TestFSMRef, TestProbe}
import ericj.chelan.raft.messages.{AppendEntriesRequest, RequestVoteResponse, RequestVoteRequest, Init}
import akka.actor.FSM.StateTimeout
import akka.actor.ActorSystem
import org.scalatest._


/**
 * Created by Eric Jutrzenka on 13/03/2014.
 */
class CandidateSpec extends TestKit(ActorSystem("test"))
with DefaultTimeout with fixture.FlatSpecLike with Matchers with
OptionValues with Inside with Inspectors with BeforeAndAfter {

  case class FixtureParam(raft: TestFSMRef[State, AllData, RaftActor], probes: Array[TestProbe], initialTerm: Int)

  override protected def withFixture(test: OneArgTest) = {
    val f = FixtureParam(TestFSMRef(new RaftActor), Array.fill(4)(TestProbe()), 0)
    f.raft ! Init(f.probes.map {
      p => p.ref
    })
    f.raft ! StateTimeout
    try {
      withFixture(test.toNoArgTest(f.copy(initialTerm = f.raft.stateData.currentTerm)))
    }
    finally {
      f.probes.foreach {
        _ => system.stop(_)
      }
      system.stop(f.raft)
    }
  }

  "A Candidate" should "send out vote requests when it starts a new term." in {
    f =>
      forAll(f.probes) {
        p =>
          p.expectMsg(RequestVoteRequest(f.initialTerm))
      }
  }
  it should "start a new term when it times out as a candidate" in {
    f =>
      f.raft ! StateTimeout
      f.raft.stateName should be(Candidate)
      f.raft.stateData.currentTerm should be(f.initialTerm + 1)
      forAll(f.probes) {
        p =>
          p.expectMsgType[RequestVoteRequest]
      }
  }
  it should "stand down if a RequestVoteRequest with greater term is received" in {
    f =>
      val initialTerm = f.raft.stateData.currentTerm
      f.raft ! RequestVoteRequest(initialTerm + 1)
      f.raft.stateName should be(Follower)
  }
  it should "stand down if a AppendEntriesRequest with greater term is received" in {
    f =>
      val initialTerm = f.raft.stateData.currentTerm
      f.raft ! AppendEntriesRequest(initialTerm + 1)
      f.raft.stateName should be(Follower)
  }
  it should "stand down if a AppendEntriesRequest with the same term is received" in {
    f =>
      val initialTerm = f.raft.stateData.currentTerm
      f.raft ! AppendEntriesRequest(initialTerm)
      f.raft.stateName should be(Follower)
  }
  it should "be elected leader when it receives enough votes" in {
    f =>
      f.probes(0).expectMsg(RequestVoteRequest(f.initialTerm))
      f.probes(0).reply(RequestVoteResponse(f.initialTerm, granted = true))
      f.raft.stateName should not be Leader
      f.probes(1).expectMsg(RequestVoteRequest(f.initialTerm))
      f.probes(1).reply(RequestVoteResponse(f.initialTerm, granted = true))
      f.raft.stateName should be(Leader)
  }
  it should "ignore votes from the same elector" in {
    f =>
      f.probes(0).expectMsg(RequestVoteRequest(f.initialTerm))
      f.probes(0).reply(RequestVoteResponse(f.initialTerm, granted = true))
      f.raft.stateName should not be Leader
      f.probes(0).expectMsg(RequestVoteRequest(f.initialTerm))
      f.probes(0).reply(RequestVoteResponse(f.initialTerm, granted = true))
      f.raft.stateName should not be Leader
  }
  it should "not count votes that are not granted" in {
    f =>
      f.probes(0).expectMsg(RequestVoteRequest(f.initialTerm))
      f.probes(0).reply(RequestVoteResponse(f.initialTerm, granted = true))
      f.raft.stateName should not be Leader
      f.probes(1).expectMsg(RequestVoteRequest(f.initialTerm))
      f.probes(1).reply(RequestVoteResponse(f.initialTerm, granted = false))
      f.raft.stateName should not be Leader
  }


}
