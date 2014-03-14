package ericj.chelan.raft

import akka.actor.{ FSM, Actor, LoggingFSM, ActorRef }
import ericj.chelan.raft.messages._
import scala.concurrent.duration._
import scala.annotation.tailrec

/**
 * Created by Eric Jutrzenka on 03/03/2014.
 */
class RaftActor extends Actor
    with LoggingFSM[State, AllData]
    with RaftBehaviour {

  val initialState = AllData(ReplicaVars(0, None), null)

  startWith(NotStarted, initialState)

  when(NotStarted) {
    case Event(Init(members), s) =>
      goto(Follower) using s.copy(
        electorate = members map { ref => Member(ref) })
  }

  when(Follower, stateTimeout = electionTimeout()) {
    startNewTerm orElse
      raftReceive
  }

  when(Candidate) {
    transform {
      standDown orElse
        startNewTerm orElse
        requestVote orElse
        raftReceive
    } using {
      case a @ FSM.State(state, s: AllData, timeout, stopReason, replies) if s.enoughVotes =>
        goto(Leader) using s.toLeaderState
    }
  }

  when(Leader) {
    raftReceive orElse
      appendEntries
  }

  onTransition {
    case Follower -> Candidate =>
      setTimer("heartBeat", HeartBeat, 100 milliseconds, repeat = true)
      setTimer("electionTimeout", ElectionTimeout, electionTimeout(), repeat = true)
    case Candidate -> Follower =>
      cancelTimer("heartBeat")
      cancelTimer("electionTimeout")
    case Candidate -> Leader =>
      cancelTimer("electionTimeout")
    case Leader -> Follower =>
      cancelTimer("heartBeat")
  }

  whenUnhandled {
    case Event(HeartBeat, s) => stay()
  }

}

sealed trait Data

case class AllData(replicaVars: ReplicaVars,
  electorate: Array[Member],
  ballots: List[Ballot] = List.empty,
  logVars: LogVars = LogVars())
    extends Data {

  def withNewTerm(newTerm: Int): AllData = {
    assert(newTerm >= replicaVars.currentTerm)
    copy(replicaVars = ReplicaVars(newTerm, None), ballots = List.empty)
  }

  def newTerm: AllData = withNewTerm(replicaVars.currentTerm + 1)

  def toLeaderState: AllData = {
    copy(electorate = electorate.map { m =>
      Member(m.ref, nextIndex = logVars.log.length, matchIndex = 0)
    })
  }

  def appendToLog(entries: List[NewEntry]): AllData = {
    val lastIndex = logVars.log.head.index
    copy(logVars = logVars.copy(log = appendToLog(lastIndex + 1, entries, logVars.log)))
  }

  @tailrec
  private def appendToLog(nextIndex: Int, entries: List[NewEntry], log: List[LogEntry]): List[LogEntry] = entries match {
    case Nil => log
    case NewEntry(term, value) :: xs => appendToLog(nextIndex + 1, xs, LogEntry(term, nextIndex, value) :: log)
  }

  def clearLogFrom(index: Int): AllData = {
    copy(logVars = logVars.copy(log = logVars.log.dropWhile(l => l.index >= index)))
  }

  def votedFor(candidate: ActorRef) = copy(replicaVars = replicaVars.copy(votedFor = Some(candidate)))

  def voted: Boolean = replicaVars.votedFor != None

  def currentTerm = replicaVars.currentTerm

  def enoughVotes: Boolean = {
    ballots.count(b => b.granted) >= electorate.length / 2
  }

  def count(ballot: Ballot): AllData =
    if (!counted(ballot.elector)) copy(ballots = ballots.+:(ballot))
    else this

  def counted(elector: ActorRef) = ballots.filter(b => b.elector == elector).nonEmpty

  def isValid(prevLogIndex: Int, prevLogTerm: Int, logVars: LogVars): Boolean = {
    logVars.log.head.index == prevLogIndex && logVars.log.head.term == prevLogTerm
  }
}

case class Ballot(elector: ActorRef, granted: Boolean)

case class ReplicaVars(currentTerm: Int, votedFor: Option[ActorRef])

case class LogVars(log: List[LogEntry] = List(LogEntry()), commitIndex: Int = 0)

case class LogEntry(term: Int = 0, index: Int = 0, value: Any = None)

case class Member(ref: ActorRef, nextIndex: Int = 0, matchIndex: Int = 0)

sealed trait State

case object Follower extends State

case object NotStarted extends State

case object Candidate extends State

case object Leader extends State

