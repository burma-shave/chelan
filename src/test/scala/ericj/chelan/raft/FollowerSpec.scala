package ericj.chelan.raft

import akka.testkit.{DefaultTimeout, TestKit, TestProbe, TestFSMRef}
import ericj.chelan.raft.messages.RequestVoteRequest
import akka.actor.FSM.StateTimeout
import akka.actor.{ActorSystem, ActorRef}
import org.scalatest._
import ericj.chelan.raft.messages.RequestVoteRequest
import ericj.chelan.raft.messages.Init

/**
 * Created by Eric Jutrzenka on 03/03/2014.
 */
class FollowerSpec extends TestKit(ActorSystem("test"))
with DefaultTimeout with fixture.FlatSpecLike with Matchers with
OptionValues with Inside with Inspectors with BeforeAndAfter {

  case class FixtureParam(raft: TestFSMRef[State, AllData, RaftActor], probes: Array[TestProbe], initialTerm: Int)

  override protected def withFixture(test: OneArgTest) = {
    val f = FixtureParam(TestFSMRef(new RaftActor), Array.fill(4)(TestProbe()), 0)
    try {
      withFixture(test.toNoArgTest(f))
    }
    finally {
      f.probes.foreach {
        _ => system.stop(_)
      }
      system.stop(f.raft)
    }
  }

  "A Follower" should "start in the NotStarted state" in {
    f =>
      f.raft.stateName should be(NotStarted)
  }
  it should "transition to Follower when initialised" in {
    f =>
      val electorate = f.probes.map {
        p => p.ref
      }
      f.raft ! Init(electorate)
      f.raft.stateName should be(Follower)
      f.raft.stateData.electorate should be(electorate)
  }
  it should "become a candidate when it times out as a follower" in {
    f =>
      val electorate = f.probes.map {
        p => p.ref
      }
      f.raft ! Init(electorate)
      f.raft ! StateTimeout
      f.raft.stateName should be(Candidate)
      f.raft.stateData.currentTerm should be(f.initialTerm + 1)
  }
}
