package ericj.chelan.raft

import akka.actor.{ FSM, Actor, LoggingFSM, ActorRef }
import ericj.chelan.raft.messages._
import scala.concurrent.duration._
import scala.annotation.tailrec

/**
 *
 * Created by Eric Jutrzenka on 03/03/2014.
 */
class RaftActor extends Actor with LoggingFSM[State, RaftState] {
  
  val initialState = RaftState(ReplicaVars(0, None), null)

  startWith(NotStarted, initialState)

  when(NotStarted) {
    case Event(Init(members), s) =>
      goto(Follower) using s.copy(
        electorate = members map {
          ref => Member(ref)
        })
  }

  when(Follower, stateTimeout = electionTimeout()) {
    handleStaleMessage orElse {
      case Event(StateTimeout, s) =>
        val newState: RaftState = s.newTerm
        s.electorate foreach (m => m.ref ! RequestVoteRequest(newState.currentTerm))
        goto(Candidate) using newState
      case Event(m: ClientRequest, s) =>
        //s.leader forward m
        stay()
      case Event(m: RaftMessage, s) =>
        gotoFollowerAndHandleMessage(m, s)
    }
  }

  when(Candidate) {
    transform {
      handleStaleMessage orElse {
        case Event(StateTimeout, s) =>
          val newState: RaftState = s.newTerm
          s.electorate foreach (m => m.ref ! RequestVoteRequest(newState.currentTerm))
          goto(Candidate) using newState
        case Event(HeartBeat, s) =>
          s.electorate foreach { m => m.ref ! RequestVoteRequest(s.currentTerm) }
          stay()
        case Event(m: ClientRequest, s) =>
          self forward m
          stay()
        case Event(RequestVoteResponse(term, granted), s) if term == s.currentTerm =>
          stay using (s count Ballot(sender(), granted))
        case Event(m: RaftMessage, s) if m.term >= s.currentTerm =>
          gotoFollowerAndHandleMessage(m, s)
      }
    } using {
      case a @ FSM.State(state, s, timeout, stopReason, replies) if s.enoughVotes =>
        goto(Leader) using s.toLeaderState
    }
  }

  when(Leader) {
    handleStaleMessage orElse {
      case Event(m: RaftMessage, s) if m.term > s.currentTerm =>
        gotoFollowerAndHandleMessage(m, s)
      case Event(HeartBeat, s) =>
        s.electorate.foreach { m =>
          m.ref ! AppendEntriesRequest(
            s.currentTerm,
            prevLogIndex = m.nextIndex - 1,
            prevLogTerm = s termOfLogEntryAtIndex (m.nextIndex - 1),
            entries = s.logVars.log.filter(p => p.index >= m.nextIndex) map (e => NewEntry(e.term, e.value)) reverse,
            leaderCommit = 0
          )
        }
        stay()
      case Event(AppendEntriesResponse(term, Some(lastAppliedIndex)), s) =>
        assert(term == s.currentTerm)
        stay() using s.updateLastAgreeIndexFor(sender(), lastAppliedIndex)
      case Event(AppendEntriesResponse(term, None), s) =>
        assert(term == s.currentTerm)
        stay() using (s decrementNextIndexFor sender())
      case Event(ClientRequest(payload), s) =>
        stay() using (s appendToLog NewEntry(s.currentTerm, payload))
    }
  }

  onTransition {
    case Follower -> Candidate =>
      setTimer("heartBeat", HeartBeat, 100 milliseconds, repeat = true)
      setTimer("electionTimeout", StateTimeout, electionTimeout(), repeat = true)
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

  private def gotoFollowerAndHandleMessage(m: RaftMessage, s: RaftState) = {
    goto(Follower) using
      (handleAppendEntriesRequest orElse handleRequestVoteRequest orElse ignore)(Event(m, s withTerm m.term))
  }

  /**
   * Drops a response where the term is less than the current term.
   * Responds with success = false to any request where the term
   * is less than the current term.
   *
   * @return The state function which does this.
   */
  private def handleStaleMessage: StateFunction = {
    case Event(m: RaftResponse, s) if m.term < s.currentTerm =>
      stay()
    case Event(m: AppendEntriesRequest, s) if m.term < s.currentTerm =>
      stay() replying AppendEntriesResponse(s.currentTerm, None)
    case Event(m: RequestVoteRequest, s) if m.term < s.currentTerm =>
      stay() replying RequestVoteResponse(s.currentTerm, false)
  }

  /**
   * Respond to a vote request. Only grant votes in the current term
   * and if not already voted.
   * @return stay in the current state and set votedFor.
   */
  private def handleRequestVoteRequest: PartialFunction[Event, RaftState] = {
    case Event(RequestVoteRequest(term), s) =>
      assert(term == s.currentTerm)
      if (s.voted) {
        sender ! RequestVoteResponse(s.currentTerm, success = false)
        s
      } else {
        sender ! RequestVoteResponse(s.currentTerm, success = true)
        s.votedFor(sender())
      }
  }

  /**
   * Performs validity check on the request and appends the entries
   * to the log. Rejects invalid requests.
   *
   * @return
   */
  private def handleAppendEntriesRequest: PartialFunction[Event, RaftState] = {
    case Event(AppendEntriesRequest(term, prevLogIndex, prevLogTerm, entries, leaderCommit), s) =>
      assert(term == s.currentTerm)
      if (s.isValid(prevLogIndex, prevLogTerm, s.logVars)) {
        sender ! AppendEntriesResponse(s.currentTerm, Some(s.logVars.log.head.index + entries.length))
        s appendToLog entries
      } else {
        sender ! AppendEntriesResponse(s.currentTerm, None)
        s clearLogFrom prevLogIndex
      }
  }

  private def ignore: PartialFunction[Event, RaftState] = {
    case Event(_, s) => s
  }

  private def electionTimeout(): FiniteDuration = (Math.random() * 150 milliseconds) + (150 milliseconds)

}

case class RaftState(replicaVars: ReplicaVars,
  electorate: Array[Member],
  ballots: List[Ballot] = List.empty,
  logVars: LogVars = LogVars()) {

  def withTerm(newTerm: Int): RaftState = {
    assert(newTerm >= replicaVars.currentTerm)
    if (newTerm > replicaVars.currentTerm)
      copy(replicaVars = ReplicaVars(newTerm, None), ballots = List.empty)
    else
      this
  }

  def newTerm: RaftState = withTerm(replicaVars.currentTerm + 1)

  def termOfLogEntryAtIndex(index: Int): Int = {
    logVars.log.collectFirst {
      case LogEntry(term, entryIndex, _) if entryIndex == index => term
    }.get
  }

  def toLeaderState: RaftState = {
    copy(electorate = electorate.map {
      m =>
        Member(m.ref, nextIndex = logVars.log.length, lastAgreeIndex = 0)
    })
  }

  def appendToLog(entry: NewEntry): RaftState = {
    appendToLog(List(entry))
  }

  def appendToLog(entries: List[NewEntry]): RaftState = {
    val lastIndex = logVars.log.head.index
    copy(logVars = logVars.copy(log = appendToLog(lastIndex + 1, entries, logVars.log)))
  }

  @tailrec
  private def appendToLog(nextIndex: Int, entries: List[NewEntry], log: List[LogEntry]): List[LogEntry] = entries match {
    case Nil => log
    case NewEntry(term, value) :: xs => appendToLog(nextIndex + 1, xs, LogEntry(term, nextIndex, value) :: log)
  }

  def clearLogFrom(index: Int): RaftState = {
    copy(logVars = logVars.copy(log = logVars.log.dropWhile(l => l.index >= index)))
  }

  def votedFor(candidate: ActorRef) = copy(replicaVars = replicaVars.copy(votedFor = Some(candidate)))

  def voted: Boolean = replicaVars.votedFor != None

  def currentTerm = replicaVars.currentTerm

  def decrementNextIndexFor(member: ActorRef): RaftState = {
    copy(electorate = electorate map {
      m => if (member == m.ref) m.decrementIndex else m
    })
  }

  def incrementNextIndexFor(member: ActorRef): RaftState = {
    copy(electorate = electorate map {
      m => if (member == m.ref && m.nextIndex <= logVars.log.head.index) m.incrementIndex else m
    })
  }

  def updateLastAgreeIndexFor(member: ActorRef, newIndex: Int): RaftState = {
    copy(electorate = electorate map {
      m => if (member == m.ref) m lastAgreeIndex newIndex else m
    })
  }

  def enoughVotes: Boolean = {
    ballots.count(b => b.granted) >= electorate.length / 2
  }

  def count(ballot: Ballot): RaftState =
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

sealed trait State

case object Follower extends State

case object NotStarted extends State

case object Candidate extends State

case object Leader extends State

