package ericj.chelan.raft

import org.scalatest._
import akka.actor._

import akka.testkit.{ TestProbe, TestKit }
import ericj.chelan.raft.messages.NewEntry

/**
 * Created by Eric Jutrzenka on 24/03/2014.
 */
class RaftStateSpec extends TestKit(ActorSystem("test")) with FreeSpecLike with Matchers {

  "foo" - {
    var foo: Int = 0

    def fooBar() {
      println(foo)
      foo = foo + 1
    }

    def fee(): PartialFunction[Any, Unit] = {
      var upd: Int = 0
      println("init fee");
      {
        case m => println(m + " " + upd); upd = upd + 1
      }
    }

    val bgfd = fee()

    bgfd("fdds")
    bgfd("fdds")
  }

  "A RaftState" - {
    "when uninitialised" - {
      "should have a single log entry" in {
        new RaftState().logVars.log should contain only LogEntry(0, 0, None)
      }
      "should have no cluster members" in {
        new RaftState().electorate shouldBe empty
      }
      "should have a term of zero" in {
        new RaftState().currentTerm shouldBe 0
      }
      "should have a leader of 'None'" in {
        new RaftState().leader should be(None)
      }
      "should have voted = false" in {
        new RaftState().voted should be(false)
      }
    }
    "when initialised" - {
      "should set its electorate" in {
        val state = new RaftState()
        val newState = state.updated(new Initialised(Array(ActorRef.noSender)))
        newState.electorate should contain only Member(ActorRef.noSender, nextIndex = 1, lastAgreeIndex = 0)
      }
      "should update its term when the term has changed" in {
        val updated: RaftState = new RaftState().updated(NewTerm(7))
        updated.currentTerm should be(7)
      }
      "should update its term to '1' when the term is Incremented" in {
        (new RaftState() x TermIncremented).currentTerm should be(1)
      }
      "should add entries to the log when the the entries have been appended" in {
        val raftState = new RaftState()
        val newState = raftState x AppendedToLog(List(NewEntry(0, "Hello"), NewEntry(1, "World")))
        newState.logVars.log should contain inOrderOnly (
          LogEntry(1, 2, "World"),
          LogEntry(0, 1, "Hello"),
          LogEntry(0, 0, None))
      }

      "should set voted to 'true' when VotedFor(ActorRef.noSender)" in {
        val state = RaftState() x VotedFor(TestProbe().ref)
        state.voted should be(true)
      }
      "should count the ballot when BallotCounted(elector, granted = true)" in {
        val probe1 = TestProbe().ref
        val probe2 = TestProbe().ref
        val state = RaftState() x
          Initialised(Array(probe1, probe2)) x
          Elected x
          BallotCounted(probe1, true)

        state.counted(probe1) should be(true)
        state.counted(probe2) should be(false)
      }
      "should count the ballot when BallotCounted(elector, granted = false)" in {
        val probe1 = TestProbe().ref
        val probe2 = TestProbe().ref
        val state = RaftState() x
          Initialised(Array(probe1, probe2)) x
          Elected x
          BallotCounted(probe1, false)

        state.counted(probe1) should be(true)
        state.counted(probe2) should be(false)
      }
      "should not have a majority if too few ballots counted" in {
        val probe1 = TestProbe().ref
        val probe2 = TestProbe().ref
        val probe3 = TestProbe().ref
        val probe4 = TestProbe().ref

        val state = RaftState() x
          Initialised(Array(probe1, probe2, probe3, probe4)) x
          Elected x
          BallotCounted(probe1, true)

        state.enoughVotes should be(false)
      }
      "should not have a majority if votes not granted" in {
        val probe1 = TestProbe().ref
        val probe2 = TestProbe().ref
        val probe3 = TestProbe().ref
        val probe4 = TestProbe().ref

        val state = RaftState() x
          Initialised(Array(probe1, probe2, probe3, probe4)) x
          Elected x
          BallotCounted(probe1, false) x
          BallotCounted(probe1, false)

        state.enoughVotes should be(false)
      }
      "should have a majority if majority votes granted" in {
        val probe1 = TestProbe().ref
        val probe2 = TestProbe().ref
        val probe3 = TestProbe().ref
        val probe4 = TestProbe().ref

        val state = RaftState() x
          Initialised(Array(probe1, probe2, probe3, probe4)) x
          Elected x
          BallotCounted(probe1, true) x
          BallotCounted(probe1, true)

        state.enoughVotes should be(false)
      }
      "should update the leader when NewLeaderDetected(newLeader)" in {
        val probe1 = TestProbe().ref
        val probe2 = TestProbe().ref
        val state = RaftState() x
          Initialised(Array(probe1, probe2)) x
          NewLeaderDetected(probe1)

        state.leader should be(Some(probe1))
      }
      "if the log is empty" - {
        "should set the nextIndex = 1, lastAgreeIndex = 0 for followers when Elected" in {
          val probe1 = TestProbe().ref
          val probe2 = TestProbe().ref
          val state = RaftState() x
            Initialised(Array(probe1, probe2)) x
            Elected

          state.electorate should contain only (Member(probe1, 1, 0), Member(probe2, 1, 0))
        }
        "should ignore AppendRejected" in {
          val probe1 = TestProbe().ref
          val probe2 = TestProbe().ref
          val state = RaftState() x
            Initialised(Array(probe1, probe2)) x
            Elected x
            AppendRejected(probe1)

          state.electorate should contain only (Member(probe1, 1, 0), Member(probe2, 1, 0))
        }
        "prevLogTerm 0, and prevLogIndex 0 should be valid" in {
          new RaftState().isValid(0, 0) should be(true)
        }
      }

      val entries = List(NewEntry(0, "Hello"), NewEntry(1, "World"))
      "if the log has " + entries.length + " entries: " + entries - {
        "should have entry with term = 1 at index 2" in {
          val probe1 = TestProbe().ref
          val probe2 = TestProbe().ref
          val state = RaftState() x
            Initialised(Array(probe1, probe2)) x
            AppendedToLog(entries)
          state.termOfLogEntryAtIndex(2) should be(1)
        }
        "should set the nextIndex = " + entries.length + 1 + ", lastAgreeIndex = 0 for followers when Elected" in {
          val probe1 = TestProbe().ref
          val probe2 = TestProbe().ref
          val state = RaftState() x
            Initialised(Array(probe1, probe2)) x
            AppendedToLog(entries) x
            Elected
          state.electorate should contain only (Member(probe1, 3, 0), Member(probe2, 3, 0))
        }
        "should set the " + entries.length + ", lastAgreeIndex = 0 for AppendRejected" in {
          val probe1 = TestProbe().ref
          val probe2 = TestProbe().ref
          val state = RaftState() x
            Initialised(Array(probe1, probe2)) x
            AppendedToLog(entries) x
            Elected x
            AppendRejected(probe1)

          state.electorate should contain only (Member(probe1, 2, 0), Member(probe2, 3, 0))
        }
        "should set nextIndex = 2, lastAgreeIndex 1 when AppendAccepted(probe1, 1)" in {
          val probe1 = TestProbe().ref
          val probe2 = TestProbe().ref
          val state = RaftState() x
            Initialised(Array(probe1, probe2)) x
            Elected x
            AppendedToLog(entries) x
            AppendAccepted(probe1, 1)
          state.electorate should contain only (Member(probe1, 2, 1), Member(probe2, 1, 0))
        }

        "should set nextIndex = 3, lastAgreeIndex 2 when AppendAccepted(probe2, 2)" in {
          val probe1 = TestProbe().ref
          val probe2 = TestProbe().ref
          val state = RaftState() x
            Initialised(Array(probe1, probe2)) x
            Elected x
            AppendedToLog(entries) x
            AppendAccepted(probe2, 2)
          state.electorate should contain only (Member(probe1, 1, 0), Member(probe2, 3, 2))
        }
        "should leave commitIndex = 0 when AppendAccepted(_, 2) for the minority of members" in {
          val probe1 = TestProbe().ref
          val probe2 = TestProbe().ref
          val probe3 = TestProbe().ref
          val probe4 = TestProbe().ref
          val probe5 = TestProbe().ref

          val state = RaftState() x
            Initialised(Array(probe1, probe2, probe3, probe4, probe5)) x
            Elected x
            AppendedToLog(entries) x
            AppendAccepted(probe2, 2) x
            AppendAccepted(probe1, 2)

          state.logVars.commitIndex should be(0)
        }
        "should update commitIndex = 2 when AppendAccepted(_, 2) for the majority of members" in {
          val probe1 = TestProbe().ref
          val probe2 = TestProbe().ref
          val probe3 = TestProbe().ref
          val probe4 = TestProbe().ref

          val state = RaftState() x
            Initialised(Array(probe1, probe2, probe3, probe4)) x
            Elected x
            AppendedToLog(entries) x
            AppendAccepted(probe2, 2) x
            AppendAccepted(probe1, 2)

          state.logVars.commitIndex should be(2)
        }
        "should clear all entries from the log when LogClearedFrom(1)" in {
          val raftState = new RaftState()
          val newState = raftState x
            AppendedToLog(entries) x
            LogClearedFrom(1)

          newState.logVars.log should contain only LogEntry(0, 0, None)
        }
        "should clear one entry from the log when LogClearedFrom(2)" in {
          val raftState = new RaftState()
          val newState = raftState x
            AppendedToLog(entries) x
            LogClearedFrom(2)

          newState.logVars.log should contain inOrder (LogEntry(0, 1, "Hello"), LogEntry(0, 0, None))
        }
        "should ignore LogClearedFrom(0)" in {
          val raftState = new RaftState()
          val newState = raftState x
            AppendedToLog(entries) x
            LogClearedFrom(0)
          newState.logVars.log should contain inOrderOnly (
            LogEntry(1, 2, "World"),
            LogEntry(0, 1, "Hello"),
            LogEntry(0, 0, None))
        }
        "should ignore LogClearedFrom(3)" in {
          val raftState = new RaftState()
          val newState = raftState x
            AppendedToLog(entries) x
            LogClearedFrom(3)

          newState.logVars.log should contain inOrderOnly (
            LogEntry(1, 2, "World"),
            LogEntry(0, 1, "Hello"),
            LogEntry(0, 0, None))
        }
        "prevLogTerm 1, and prevLogIndex 2 should be valid" in {
          val raftState = new RaftState()
          val newState = raftState x
            AppendedToLog(entries) x
            LogClearedFrom(3)

          newState.isValid(2, 1) should be(true)
        }

      }
    }
  }

}

