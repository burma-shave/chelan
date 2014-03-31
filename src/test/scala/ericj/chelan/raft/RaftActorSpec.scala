package ericj.chelan.raft

import akka.testkit.{ ImplicitSender, TestProbe, TestKit }
import akka.actor.{ ReceiveTimeout, ActorRef, ActorSystem }
import org.scalatest._
import ericj.chelan.raft.messages._
import java.util.UUID
import ericj.chelan.raft.messages.AppendEntriesResponse
import ericj.chelan.raft.messages.NewEntry
import ericj.chelan.raft.messages.Init
import ericj.chelan.raft.messages.AppendEntriesRequest

import scala.Some

/**
 *
 * Created by Eric Jutrzenka on 21/03/2014.
 */
class RaftActorSpec extends TestKit(ActorSystem("test")) with fixture.FreeSpecLike with Matchers with ImplicitSender with BeforeAndAfterAll {

  case class FixtureParam(raftActor: ActorRef, cluster: Array[TestProbe]) {
    def initRaftActor() = {
      raftActor ! Init(cluster map (_.ref))
      expectMsgType[Initialised]
    }

    def becomeCandidate() = {
      initRaftActor()
      raftActor ! ReceiveTimeout
      expectMsg(TermIncremented)

      cluster foreach {
        c => c.expectMsg(RequestVoteRequest(1))
      }

    }

    def becomeLeader() = {
      becomeCandidate()

      (cluster take 2) foreach {
        member =>
          member.send(raftActor, RequestVoteResponse(1, success = true))
          expectMsg(BallotCounted(member.ref, granted = true))
      }
      expectMsg(Elected)
      cluster foreach {
        m => m.expectMsg(AppendEntriesRequest(1, 0, 0, List.empty, 0))
      }
    }

    def addToLog(entries: List[NewEntry]) = {
      raftActor ! AppendEntriesRequest(0, 0, 0, entries, 0)
      expectMsgType[NewLeaderDetected]
      expectMsgType[AppendedToLog]
      expectMsgType[AppendEntriesResponse]
    }
  }

  override def beforeAll {
    system.eventStream.subscribe(testActor, classOf[StateEvent])
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "A Follower" - {
    "must emit an Initialised event when it receives an Init" in {
      f =>
        val refs = f.cluster map {
          _.ref
        }
        f.raftActor ! Init(refs)
        expectMsg(Initialised(refs))
    }
    "when it receives" - {
      "an AppendEntriesRequest" - {
        "that is valid, it must append the entries and respond with the last agreeIndex" in { f =>
          f.initRaftActor()
          val newEntries = List(NewEntry(0, "Hello"))
          f.raftActor ! AppendEntriesRequest(0, 0, 0, newEntries, 0)
          expectMsgType[NewLeaderDetected]
          expectMsg(AppendedToLog(newEntries))
          expectMsg(AppendEntriesResponse(0, Some(1)))
        }
        "that has term > currentTerm, it must update it's term and append the entries" in { f =>
          f.initRaftActor()
          val newEntries = List(NewEntry(0, "Hello"))
          f.raftActor ! AppendEntriesRequest(1, 0, 0, newEntries, 0)
          expectMsg(NewTerm(1))
          expectMsg(NewLeaderDetected(testActor))
          expectMsg(AppendedToLog(newEntries))
          expectMsg(AppendEntriesResponse(1, Some(1)))
        }
        "that has conflicting entries, it must respond with failure and clear conflicting log entries" in { f =>
          f.initRaftActor()
          f.addToLog(List(NewEntry(0, 1), NewEntry(0, 2)))
          f.raftActor ! AppendEntriesRequest(0, 2, 1, List.empty, 0)
          expectMsg(LogClearedFrom(2))
          expectMsg(AppendEntriesResponse(0, None))
        }
        "that has entries past the end of the log, it must respond with failure" in { f =>
          f.initRaftActor()
          f.addToLog(List(NewEntry(0, 1), NewEntry(0, 2)))
          f.raftActor ! AppendEntriesRequest(0, 2, 1, List.empty, 0)
          expectMsg(LogClearedFrom(2))
          expectMsg(AppendEntriesResponse(0, None))
        }
      }

      "a RequestVoteRequest" - {
        "and it has not already voted, it must grant the vote" in { f =>
          f.initRaftActor()
          f.cluster(0).send(f.raftActor, RequestVoteRequest(1))
          expectMsg(NewTerm(1))
          expectMsg(VotedFor(f.cluster(0).ref))
          f.cluster(0).expectMsg(RequestVoteResponse(1, success = true))
        }
        "and it has already voted in this term, it must not grant the vote" in { f =>
          f.initRaftActor()
          f.cluster(0).send(f.raftActor, RequestVoteRequest(1))
          expectMsg(NewTerm(1))
          expectMsg(VotedFor(f.cluster(0).ref))
          f.cluster(0).expectMsg(RequestVoteResponse(1, success = true))
          f.cluster(1).send(f.raftActor, RequestVoteRequest(1))
          f.cluster(1).expectMsg(RequestVoteResponse(1, success = false))
        }
        "in term = 2 and it has voted term = 1, it must grant the vote" in { f =>
          f.initRaftActor()
          f.cluster(0).send(f.raftActor, RequestVoteRequest(1))
          expectMsg(NewTerm(1))
          expectMsg(VotedFor(f.cluster(0).ref))
          f.cluster(0).expectMsg(RequestVoteResponse(1, success = true))
          f.cluster(1).send(f.raftActor, RequestVoteRequest(2))
          f.cluster(1).expectMsg(RequestVoteResponse(2, success = true))
          expectMsg(NewTerm(2))
          expectMsg(VotedFor(f.cluster(1).ref))
        }
        "that has smaller lastLogTerm, it must not grant the vote" in {
          f =>
            f.initRaftActor()
            f.raftActor ! AppendEntriesRequest(5, 0, 0, List(NewEntry(5, "Foo")), 0)
            expectMsg(NewTerm(5))
            expectMsgType[NewLeaderDetected]
            expectMsgType[AppendedToLog]
            expectMsgType[AppendEntriesResponse]

            f.cluster(0).send(f.raftActor, RequestVoteRequest(5, lastLogTerm = 4, lastLogIndex = 0))
            f.cluster(0).expectMsg(RequestVoteResponse(5, success = false))
        }
        "that has same term but smaller lastLogIndex, it must not grant the vote" in {
          f =>
            f.initRaftActor()
            f.raftActor ! AppendEntriesRequest(5, 0, 0, List(NewEntry(5, "Foo"), NewEntry(5, "Bar")), 0)
            expectMsg(NewTerm(5))
            expectMsgType[NewLeaderDetected]
            expectMsgType[AppendedToLog]
            expectMsgType[AppendEntriesResponse]

            f.cluster(0).send(f.raftActor, RequestVoteRequest(5, lastLogIndex = 1, lastLogTerm = 5))
            f.cluster(0).expectMsg(RequestVoteResponse(5, success = false))
        }
        "that has greater term but smaller lastLogTerm, it must grant the vote" in {
          f =>
            f.initRaftActor()
            f.raftActor ! AppendEntriesRequest(5, 0, 0, List(NewEntry(5, "Foo"), NewEntry(5, "Bar")), 0)
            expectMsg(NewTerm(5))
            expectMsgType[NewLeaderDetected]
            expectMsgType[AppendedToLog]
            expectMsgType[AppendEntriesResponse]

            f.cluster(0).send(f.raftActor, RequestVoteRequest(5, lastLogIndex = 2, lastLogTerm = 6))
            expectMsg(VotedFor(f.cluster(0).ref))
            f.cluster(0).expectMsg(RequestVoteResponse(5, success = true))
        }
        "that has same term but greater lastLogTerm, it must grant the vote" in {
          f =>
            f.initRaftActor()
            f.raftActor ! AppendEntriesRequest(5, 0, 0, List(NewEntry(5, "Foo"), NewEntry(5, "Bar")), 0)
            expectMsg(NewTerm(5))
            expectMsgType[NewLeaderDetected]
            expectMsgType[AppendedToLog]
            expectMsgType[AppendEntriesResponse]

            f.cluster(0).send(f.raftActor, RequestVoteRequest(5, lastLogIndex = 5, lastLogTerm = 5))
            expectMsg(VotedFor(f.cluster(0).ref))

            f.cluster(0).expectMsg(RequestVoteResponse(5, success = true))
        }
      }
      "a ClientRequest, it must forward it to the leader" in { f =>
        f.initRaftActor()
        val leader = f.cluster(0)
        leader.send(f.raftActor, AppendEntriesRequest(0, 0, 0, List.empty, 0))
        expectMsgType[NewLeaderDetected]
        leader.expectMsg(AppendEntriesResponse(0, Some(0)))
        f.raftActor ! ClientRequest("foo")
        leader.expectMsg(ClientRequest("foo"))
      }
      "a ReceiveTimout, it must become a candidate and send out vote requests" in { f =>
        f.initRaftActor()
        f.raftActor ! ReceiveTimeout
        expectMsg(TermIncremented)
        f.cluster foreach {
          member => member.expectMsg(RequestVoteRequest(1))
        }
      }
    }
  }

  "A Candidate" - {
    "when it receives" - {
      "an ReceiveTimout, it must start a new term and send out vote requests" in { f =>
        f.becomeCandidate()
        f.raftActor ! ReceiveTimeout
        expectMsg(TermIncremented)
        f.cluster foreach {
          member => member.expectMsg(RequestVoteRequest(2))
        }
      }
      "a RequestVoteResponse" - {
        "that is successful, it should count the ballot" in {
          f =>
            f.becomeCandidate()
            f.cluster(0).send(f.raftActor, RequestVoteResponse(1, success = true))
            expectMsg(BallotCounted(f.cluster(0).ref, granted = true))
        }
        "that is unsuccessful, it should count the ballot" in {
          f =>
            f.becomeCandidate()
            f.cluster(0).send(f.raftActor, RequestVoteResponse(1, success = false))
            expectMsg(BallotCounted(f.cluster(0).ref, granted = false))
        }
        "with granted = true from a majority, it should become leader" in { f =>
          f.becomeCandidate()
          f.cluster(0).send(f.raftActor, RequestVoteResponse(1, success = true))
          f.cluster(1).send(f.raftActor, RequestVoteResponse(1, success = true))
          expectMsg(BallotCounted(f.cluster(0).ref, granted = true))
          expectMsg(BallotCounted(f.cluster(1).ref, granted = true))
          expectMsg(Elected)
        }
      }
      "an AppendEntriesRequest" - {
        "in the same term, it should stand-down" in { f =>
          f.becomeCandidate()
          f.cluster(0).send(f.raftActor, AppendEntriesRequest(1, 0, 0, List.empty, 0))
          expectMsg(NewLeaderDetected(f.cluster(0).ref))
          f.cluster(0).expectMsg(AppendEntriesResponse(1, Some(0)))
        }
        "in greater term, it should stand-down and update the term" in { f =>
          f.becomeCandidate()
          f.cluster(0).send(f.raftActor, AppendEntriesRequest(2, 0, 0, List.empty, 0))
          expectMsg(NewTerm(2))
          expectMsg(NewLeaderDetected(f.cluster(0).ref))
          f.cluster(0).expectMsg(AppendEntriesResponse(2, Some(0)))
        }
      }
    }
  }

  "A Leader" - {
    "should send out AppendEntriesRequests" in { f =>
      f.becomeCandidate()

      (f.cluster take 2) foreach {
        member =>
          member.send(f.raftActor, RequestVoteResponse(1, success = true))
          expectMsg(BallotCounted(member.ref, granted = true))
      }
      expectMsg(Elected)
      f.cluster foreach {
        m => m.expectMsg(AppendEntriesRequest(1, 0, 0, List.empty, 0))
      }
    }
    "when it receives" - {
      "successful AppendEntriesResponse, it send new prevLogIndex, and prevLogTerm" in { f =>
        f.becomeLeader()
        f.raftActor ! ClientRequest(1)

        expectMsg(AppendedToLog(List(NewEntry(1, 1))))

        f.cluster foreach {
          m => m.expectMsg(AppendEntriesRequest(1, 0, 0, List(NewEntry(1, 1)), 0))
        }
        f.cluster(0).send(f.raftActor, AppendEntriesResponse(1, Some(1)))
        expectMsg(AppendAccepted(f.cluster(0).ref, 1))
        f.raftActor ! ClientRequest(2)
        expectMsg(AppendedToLog(List(NewEntry(1, 2))))

        f.cluster(0).expectMsg(AppendEntriesRequest(1, 1, 1, List(NewEntry(1, 2)), 0))
      }
      "unsuccessful AppendEntriesResponse, it should decrement the nextIndex for that follower" in { f =>
        f.becomeLeader()

        f.raftActor ! ClientRequest(1)
        f.raftActor ! ClientRequest(2)
        f.raftActor ! ClientRequest(3)

        expectMsg(AppendedToLog(List(NewEntry(1, 1))))
        expectMsg(AppendedToLog(List(NewEntry(1, 2))))
        expectMsg(AppendedToLog(List(NewEntry(1, 3))))

        f.cluster(0).send(f.raftActor, AppendEntriesResponse(1, Some(3)))

        expectMsg(AppendAccepted(f.cluster(0).ref, 3))
        f.cluster(0).send(f.raftActor, AppendEntriesResponse(1, None))

        expectMsg(AppendRejected(f.cluster(0).ref))

        f.cluster(0).expectMsg(AppendEntriesRequest(1, 2, 1, List(NewEntry(1, 3)), 0))
      }
      "a ClientRequest, it should add included entries to the log" in { f =>
        f.becomeLeader()
        f.raftActor ! ClientRequest(1)

        expectMsg(AppendedToLog(List(NewEntry(1, 1))))
        f.cluster foreach {
          m => m.expectMsg(AppendEntriesRequest(1, 0, 0, List(NewEntry(1, 1)), 0))
        }
        f.raftActor ! ClientRequest(2)
        expectMsg(AppendedToLog(List(NewEntry(1, 2))))

        f.cluster foreach {
          m => m.expectMsg(AppendEntriesRequest(1, 0, 0, List(NewEntry(1, 1), NewEntry(1, 2)), 0))
        }
      }
      "a message in a newer term, it should stand down" in { f =>
        f.becomeLeader()
        f.cluster(0).ignoreMsg {
          case m: AppendEntriesResponse => false
          case _ => true
        }
        f.cluster(0).send(f.raftActor, AppendEntriesRequest(3, 3, 3, List.empty, 0))
        expectMsg(NewTerm(3))
        f.cluster(0).expectMsgType[AppendEntriesResponse]
      }
    }
  }

  def withFixture(test: OneArgTest): Outcome = {
    val f =
      FixtureParam(system.actorOf(RaftActor.props(UUID.randomUUID().toString)), Array.fill(4)(TestProbe()))
    try {
      withFixture(test.toNoArgTest(f)) // "loan" the fixture to the test
    } finally {
      system.stop(f.raftActor)
      f.cluster foreach { p => system.stop(p.ref) }
    }
  }
}
