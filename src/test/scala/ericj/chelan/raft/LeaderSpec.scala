package ericj.chelan.raft

import akka.testkit.{ TestProbe, TestFSMRef, TestKit }
import ericj.chelan.raft.messages._
import CustomMatchers._
import scala.collection._
import ericj.chelan.raft.messages.AppendEntriesRequest

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
          m.lastAgreeIndex should be(0)
      }
  }
  it should "stand down with new term when it receives a message with a newer term" in {
    f =>
      f.server ! RequestVoteRequest(f.initialTerm + 1)
      f.server should beInState(Follower)
      f.server should beInTerm(f.initialTerm + 1)
  }
  it should "send heartbeat AppendEntriesRquests to other members" in {
    f =>
      forAll(f.probes) {
        p =>
          p.fishForMessage() {
            case AppendEntriesRequest(term, prevLogIndex, prevLogTerm, entries, leaderCommit) if term == f.initialTerm &&
              prevLogIndex == 0 &&
              prevLogTerm == 0 &&
              entries == List.empty &&
              leaderCommit == 0 => true
            case _ => false
          }
      }
      forAll(f.probes) {
        p =>
          p.fishForMessage() {
            case AppendEntriesRequest(term, prevLogIndex, prevLogTerm, entries, leaderCommit) if term == f.initialTerm &&
              prevLogIndex == 0 &&
              prevLogTerm == 0 &&
              entries == List.empty &&
              leaderCommit == 0 => true
            case _ => false
          }
      }
  }
  it should "include the correct prevLogIndex and prevLogTerm heartbeat AppendEntriesRequests" in {
    f =>
      f.server ! ClientRequest("Buy some chips1")
      f.server ! ClientRequest("Buy some chips2")
      f.probes foreach {
        p => p.send(f.server, AppendEntriesResponse(f.initialTerm, Some(1)))
      }
      forAll(f.probes) {
        p =>
          p.fishForMessage() {
            case AppendEntriesRequest(term, prevLogIndex, prevLogTerm, entries, leaderCommit) if term == f.initialTerm &&
              prevLogIndex == 1 &&
              prevLogTerm == f.initialTerm &&
              entries == List(NewEntry(f.initialTerm, "Buy some chips2")) &&
              leaderCommit == 0 => true
            case _ => false
          }
      }
      f.probes foreach {
        p => p.send(f.server, AppendEntriesResponse(f.initialTerm, Some(2)))
      }
      forAll(f.probes) {
        p =>
          p.fishForMessage() {
            case AppendEntriesRequest(term, prevLogIndex, prevLogTerm, entries, leaderCommit) if term == f.initialTerm &&
              prevLogIndex == 2 &&
              prevLogTerm == f.initialTerm &&
              entries == List.empty &&
              leaderCommit == 0 => true
            case _ => false
          }
      }

  }
  it should "include entries in the correct order in AppendEntriesRequests" in {
    f =>
      f.server ! ClientRequest(1)
      f.server ! ClientRequest(2)
      f.server ! ClientRequest(3)
      forAll(f.probes) {
        p =>
          p.fishForMessage() {
            case AppendEntriesRequest(term, prevLogIndex, prevLogTerm, entries, leaderCommit) if term == f.initialTerm &&
              prevLogIndex == 0 &&
              prevLogTerm == 0 &&
              entries == List(
                NewEntry(1, 1),
                NewEntry(1, 2),
                NewEntry(1, 3)
              ) &&
                leaderCommit == 0 => true
            case _ => false
          }
      }
  }
  it should "append entries from client requests to its log" in {
    f =>
      f.server ! ClientRequest("Buy some chips")
      f.server.stateData.logVars.log should contain(LogEntry(f.initialTerm, 1, "Buy some chips"))
  }
  it should "send new entries to followers when received from client" in {
    f => pending
  }
  it should "set the matchIndex and nextIndex for a follower if entries are accepted" in {
    f =>
      val expectedAgreeIndex: Int = 2
      f.probes(0).send(f.server, AppendEntriesResponse(f.initialTerm, Some(expectedAgreeIndex)))
      val member = f.server.stateData.electorate.collectFirst {
        case m: Member if m.ref == f.probes(0).ref => m
      }.get

      member.nextIndex should be(expectedAgreeIndex + 1)
      member.lastAgreeIndex should be(expectedAgreeIndex)
  }
  it should "decrement the nextIndex for a follower if log inconsistent" in {
    f =>
      val expectedAgreeIndex: Int = 7
      f.probes(0).send(f.server, AppendEntriesResponse(f.initialTerm, Some(expectedAgreeIndex)))
      f.probes(0).send(f.server, AppendEntriesResponse(f.initialTerm, None))

      getMember.nextIndex should be(expectedAgreeIndex)

      f.probes(0).send(f.server, AppendEntriesResponse(f.initialTerm, None))

      getMember.nextIndex should be(expectedAgreeIndex - 1)

      def getMember: Member = {
        f.server.stateData.electorate.collectFirst {
          case m: Member if m.ref == f.probes(0).ref => m
        }.get
      }
  }
  it should "update commitIndex if the majority of followers have applied entries" in {
    f => pending
  }

  override def init(f: FixtureParam): FixtureParam = {
    f.becomeLeader()
    f
  }

}
