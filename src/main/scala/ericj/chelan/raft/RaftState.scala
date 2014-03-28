package ericj.chelan.raft

import ericj.chelan.raft.messages.NewEntry
import scala.annotation.tailrec
import akka.actor.ActorRef

/**
 *
 * Created by Eric Jutrzenka on 20/03/2014.
 */
case class RaftState(replicaVars: ReplicaVars = ReplicaVars(0, None),
    electorate: Array[Member] = Array.empty,
    ballots: List[Ballot] = List.empty,
    logVars: LogVars = LogVars(),
    leader: Option[ActorRef] = None) {

  def updated(evt: StateEvent): RaftState = {
    evt match {
      case Initialised(cluster) =>
        copy(electorate = cluster map { ref => Member(ref) })
      case NewTerm(term) =>
        withTerm(term)
      case TermIncremented =>
        newTerm
      case AppendedToLog(entries) =>
        appendToLog(entries)
      case LogClearedFrom(index) =>
        clearLogFrom(index)
      case VotedFor(candidate) =>
        votedFor(candidate)
      case Elected =>
        toLeaderState
      case AppendRejected(follower) =>
        decrementNextIndexFor(follower)
      case AppendAccepted(follower, matchIndex) =>
        updateLastAgreeIndexFor(follower, matchIndex)
      case BallotCounted(elector, granted) =>
        count(Ballot(elector, granted))
      case NewLeaderDetected(newLeader) =>
        copy(leader = Some(newLeader))
    }
  }

  def x(evt: StateEvent) = updated(evt)

  def termOfLogEntryAtIndex(index: Int): Int = {
    logVars.log.collectFirst {
      case LogEntry(term, entryIndex, _) if entryIndex == index => term
    }.get
  }

  def voted: Boolean = replicaVars.votedFor != None

  def currentTerm = replicaVars.currentTerm

  def enoughVotes: Boolean = {
    ballots.count(b => b.granted) >= electorate.length / 2
  }

  def counted(elector: ActorRef) = ballots.filter(b => b.elector == elector).nonEmpty

  def isValid(prevLogIndex: Int, prevLogTerm: Int): Boolean = {
    logVars.log.head.index == prevLogIndex && logVars.log.head.term == prevLogTerm
  }

  private def count(ballot: Ballot): RaftState =
    if (!counted(ballot.elector)) copy(ballots = ballots.+:(ballot))
    else this

  private def updateLastAgreeIndexFor(member: ActorRef, newIndex: Int): RaftState = {
    val newState = copy(electorate = electorate map {
      m => if (member == m.ref) m lastAgreeIndex newIndex else m
    })

    val newCommitIndx: Option[Int] = newState.electorate.toList.groupBy(m => m.lastAgreeIndex) collectFirst {
      case (lastAgreeIndex, members) if members.length + 1 > (electorate.length + 1) / 2 =>
        lastAgreeIndex
    }

    if (newCommitIndx != None && newCommitIndx.get != logVars.commitIndex)
      newState.copy(logVars = newState.logVars.copy(commitIndex = newCommitIndx.get))
    else newState
  }

  private def decrementNextIndexFor(member: ActorRef): RaftState = {
    copy(electorate = electorate map {
      m => if (member == m.ref && m.nextIndex > 1) m.decrementIndex else m
    })
  }

  private def votedFor(candidate: ActorRef) = copy(replicaVars = replicaVars.copy(votedFor = Some(candidate)))

  private def clearLogFrom(index: Int): RaftState = {
    if (index >= 1 && index < logVars.log.length - 1)
      copy(logVars = logVars.copy(log = logVars.log.dropWhile(l => l.index >= index)))
    else this
  }

  private def appendToLog(entry: NewEntry): RaftState = {
    appendToLog(List(entry))
  }

  private def appendToLog(entries: List[NewEntry]): RaftState = {
    val lastIndex = logVars.log.head.index
    copy(logVars = logVars.copy(log = appendToLog(lastIndex + 1, entries, logVars.log)))
  }

  private def withTerm(newTerm: Int): RaftState = {
    assert(newTerm >= replicaVars.currentTerm)
    if (newTerm > replicaVars.currentTerm)
      copy(replicaVars = ReplicaVars(newTerm, None), ballots = List.empty)
    else
      this
  }

  private def newTerm: RaftState = withTerm(replicaVars.currentTerm + 1)

  private def toLeaderState: RaftState = {
    copy(electorate = electorate.map {
      m =>
        Member(m.ref, nextIndex = logVars.log.length, lastAgreeIndex = 0)
    })
  }

  @tailrec
  private def appendToLog(nextIndex: Int, entries: List[NewEntry], log: List[LogEntry]): List[LogEntry] = entries match {
    case Nil => log
    case NewEntry(term, value) :: xs => appendToLog(nextIndex + 1, xs, LogEntry(term, nextIndex, value) :: log)
  }

}

sealed trait StateEvent

case class Initialised(cluster: Array[ActorRef]) extends StateEvent

case class NewTerm(term: Int) extends StateEvent

case object TermIncremented extends StateEvent

case object Elected extends StateEvent

case class NewLeaderDetected(leader: ActorRef) extends StateEvent

case class AppendedToLog(entries: List[NewEntry]) extends StateEvent

case class LogClearedFrom(index: Int) extends StateEvent

case class VotedFor(candidate: ActorRef) extends StateEvent

case class AppendRejected(follower: ActorRef) extends StateEvent

case class AppendAccepted(follower: ActorRef, matchIndex: Int) extends StateEvent

case class BallotCounted(elector: ActorRef, granted: Boolean) extends StateEvent

case class Ballot(elector: ActorRef, granted: Boolean)

case class ReplicaVars(currentTerm: Int, votedFor: Option[ActorRef])

case class LogVars(log: List[LogEntry] = List(LogEntry()), commitIndex: Int = 0)

case class LogEntry(term: Int = 0, index: Int = 0, value: Any = None)

case class Member(ref: ActorRef, nextIndex: Int = 1, lastAgreeIndex: Int = 0) {
  def decrementIndex: Member = {
    copy(nextIndex = nextIndex - 1)
  }

  def incrementIndex: Member = {
    copy(nextIndex = nextIndex + 1)
  }

  def lastAgreeIndex(index: Int): Member = {
    copy(nextIndex = index + 1, lastAgreeIndex = index)
  }
}