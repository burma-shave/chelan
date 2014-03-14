package ericj.chelan.raft

import akka.testkit.{ TestProbe, TestFSMRef, DefaultTimeout, TestKit }
import akka.actor.ActorSystem
import org.scalatest._
import ericj.chelan.raft.messages.{ RequestVoteResponse, RequestVoteRequest, Init }
import akka.actor.FSM.StateTimeout
import CustomMatchers._

/**
 * Created by Eric Jutrzenka on 14/03/2014.
 */
abstract class RaftSpec extends TestKit(ActorSystem("test"))
    with DefaultTimeout with fixture.FlatSpecLike with Matchers with OptionValues with Inside with Inspectors with BeforeAndAfter {

  case class FixtureParam(server: TestFSMRef[State, AllData, RaftActor], probes: Array[TestProbe], initialTerm: Int) {
    def becomeFollower(): Unit = {
      server ! Init(probes.map {
        p => p.ref
      })
    }
    def becomeCandidate(): Unit = {
      becomeFollower()
      server ! StateTimeout
    }
    def becomeLeader(): Unit = {
      becomeCandidate()
      probes(0).expectMsgType[RequestVoteRequest]
      probes(0).reply(RequestVoteResponse(server.stateData.currentTerm, granted = true))
      probes(1).expectMsgType[RequestVoteRequest]
      probes(1).reply(RequestVoteResponse(server.stateData.currentTerm, granted = true))
    }
  }

  def init(f: FixtureParam): FixtureParam

  override protected def withFixture(test: OneArgTest) = {
    val f = init(FixtureParam(TestFSMRef(new RaftActor), Array.fill(4)(TestProbe()), 0))

    try {
      withFixture(test.toNoArgTest(f.copy(initialTerm = f.server.stateData.currentTerm)))
    } finally {
      f.probes.foreach {
        _ => system.stop(_)
      }
      system.stop(f.server)
    }
  }

}

import org.scalatest._
import matchers._

trait CustomMatchers {

  class StateShouldBeMatcher(expectedState: Any) extends Matcher[TestFSMRef[State, AllData, RaftActor]] {

    override def apply(left: TestFSMRef[State, AllData, RaftActor]): MatchResult = {
      val stateName = left.stateName
      MatchResult(
        stateName == expectedState,
        s"FSM was in $stateName rather than $expectedState",
        s"FSM was in $expectedState"
      )
    }

  }

  class ShouldHaveTermMatcher(expectedTerm: Int) extends Matcher[TestFSMRef[State, AllData, RaftActor]] {

    override def apply(left: TestFSMRef[State, AllData, RaftActor]): MatchResult = {
      val term = left.stateData.currentTerm
      MatchResult(
        term == expectedTerm,
        s"FSM was in $term rather than $expectedTerm",
        s"FSM was in $expectedTerm"
      )
    }

  }

  def beInState(expectedState: Any) = new StateShouldBeMatcher(expectedState)

  def beInTerm(expectedTerm: Int) = new ShouldHaveTermMatcher(expectedTerm)

}

object CustomMatchers extends CustomMatchers
