package ericj.chelan.raft

import akka.testkit.{ DefaultTimeout, TestKit, TestFSMRef, TestProbe }
import ericj.chelan.raft.messages.{ AppendEntriesRequest, RequestVoteResponse, RequestVoteRequest, Init }
import akka.actor.FSM.StateTimeout
import akka.actor.ActorSystem
import org.scalatest._
import CustomMatchers._
import scala.concurrent.duration._
import scala.collection._

/**
 * Created by Eric Jutrzenka on 14/03/2014.
 */
class LeaderSpec extends RaftSpec {

  "A Leader" should "have nextIndexes initialised to log.length + 1" in {
    f =>
      f.server should beInState(Leader)
      forAll(f.server.stateData.electorate) {
        m =>
          m.nextIndex should be(f.server.stateData.logVars.log.length)
      }
  }
  it should "have matchIndexes initialised to 0" in {
    f =>
      forAll(f.server.stateData.electorate) {
        m =>
          m.matchIndex should be(0)
      }
  }
  it should "send heartbeat AppendEntriesRquests to other members" in {
    f =>
      forAll(f.probes) {
        p =>
          p.fishForMessage() {
            case m: AppendEntriesRequest => true
            case _ => false
          }
      }
      forAll(f.probes) {
        p =>
          p.fishForMessage() {
            case m: AppendEntriesRequest => true
            case _ => false
          }
      }
  }
  it should "append entries from client requests to its log" in {
    f => pending
  }
  it should "send new entries to followers when received from client" in {
    f => pending
  }
  it should "set the matchIndex for a follower if entries are accepted" in {
    f => pending
  }
  it should "decrement the nextIndex for a follower if log inconsistent" in {
    f => pending
  }
  it should "update commitIndex if the majority of followers have applied entries" in {
    f => pending
  }
  override def init(f: FixtureParam): FixtureParam = {
    f.becomeLeader()
    f
  }

}
