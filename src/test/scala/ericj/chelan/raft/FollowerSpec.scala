package ericj.chelan.raft

import akka.testkit.{ DefaultTimeout, TestKit, TestProbe, TestFSMRef }
import ericj.chelan.raft.messages._
import akka.actor.FSM.StateTimeout
import akka.actor.{ ActorSystem, ActorRef }
import org.scalatest._

/**
 *
 * Created by Eric Jutrzenka on 03/03/2014.
 */
class FollowerSpec extends RaftSpec {

  "A Follower" should "start in the NotStarted state" in {
    f =>
      f.server.stateName should be(NotStarted)
  }
  it should "transition to Follower when initialised" in {
    f =>
      val electorate = f.probes.map {
        p => p.ref
      }
      f.server ! Init(electorate)
      f.server.stateName should be(Follower)
      f.server.stateData.electorate map { m => m.ref } should be(electorate)
  }
  it should "become a candidate when it times out as a follower" in {
    f =>
      val electorate = f.probes.map {
        p => p.ref
      }
      f.server ! Init(electorate)
      f.server ! StateTimeout
      f.server.stateName should be(Candidate)
      f.server.stateData.currentTerm should be(f.initialTerm + 1)
  }
  it should "reject AppendEntriesRequest(s) where prevLogIndex does not match" in {
    f =>
      f.becomeFollower()
      val prevStateData = f.server.stateData
      f.probes(0).send(f.server, AppendEntriesRequest(
        term = f.initialTerm,
        prevLogIndex = 7,
        prevLogTerm = 0,
        entries = List.empty,
        leaderCommit = 0))
      f.probes(0).expectMsg(AppendEntriesResponse(f.initialTerm, None))
      prevStateData should equal(f.server.stateData)
  }
  it should "reject AppendEntriesRequest(s) where term is less than current term" in {
    f =>
      f.becomeFollower()
      val prevStateData = f.server.stateData
      f.probes(0).send(f.server, AppendEntriesRequest(
        term = f.initialTerm - 1,
        prevLogIndex = 7,
        prevLogTerm = 0,
        entries = List.empty,
        leaderCommit = 0))
      f.probes(0).expectMsg(AppendEntriesResponse(f.initialTerm, None))
      prevStateData should equal(f.server.stateData)
  }
  it should "append entries in a valid AppendEntriesRequest" in {
    f =>
      f.becomeFollower()
      val prevStateData = f.server.stateData
      val expectedLogEntry = 123
      f.probes(0).send(f.server, AppendEntriesRequest(
        term = f.initialTerm,
        prevLogIndex = 0,
        prevLogTerm = 0,
        entries = List(NewEntry(f.initialTerm, expectedLogEntry)),
        leaderCommit = 0))
      f.probes(0).expectMsg(AppendEntriesResponse(f.initialTerm, Some(1)))
      f.server.stateData.logVars.log should
        equal(LogEntry(f.initialTerm, 1, expectedLogEntry) :: prevStateData.logVars.log)
  }
  it should "remove conflicting entries if prevLogTerm differs for entry" in {
    f =>
      f.becomeFollower()
      f.probes(0).send(f.server, AppendEntriesRequest(
        term = 0,
        prevLogIndex = 0,
        prevLogTerm = 0,
        entries = List(NewEntry(0, 1)),
        leaderCommit = 0))
      f.probes(0).send(f.server, AppendEntriesRequest(
        term = 0,
        prevLogIndex = 1,
        prevLogTerm = 0,
        entries = List(NewEntry(0, 2)),
        leaderCommit = 0))
      f.probes(0).send(f.server, AppendEntriesRequest(
        term = 0,
        prevLogIndex = 2,
        prevLogTerm = 0,
        entries = List(NewEntry(0, 3)),
        leaderCommit = 0))
      f.probes(0).send(f.server, AppendEntriesRequest(
        term = 3,
        prevLogIndex = 2,
        prevLogTerm = 1,
        entries = List(NewEntry(1, 10), NewEntry(2, 11), NewEntry(3, 12)),
        leaderCommit = 0))
      f.server.stateData.logVars.log should
        equal(
          List(
            LogEntry(0, 0, None),
            LogEntry(0, 1, 1)
          ).reverse)
  }
  it should "commit entries if leaderCommit is ahead" in {
    f => pending
  }
  it should "respond to vote RequestVoteRequest(s) in current term" in {
    f =>
      f.becomeFollower()
      f.probes(0).send(f.server, RequestVoteRequest(
        term = 0
      ))
      f.probes(0).expectMsg(RequestVoteResponse(0, success = true))
  }
  it should "vote for only one candidate in the current term" in {
    f =>
      f.becomeFollower()
      f.probes(0).send(f.server, RequestVoteRequest(
        term = 0
      ))
      f.probes(1).send(f.server, RequestVoteRequest(
        term = 0
      ))
      f.probes(1).expectMsg(RequestVoteResponse(0, success = false))
  }
  it should "forward ClientRequests to the Leader" in {
    f => pending
  }

  override def init(f: FixtureParam): FixtureParam = f
}
