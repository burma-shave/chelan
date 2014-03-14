package ericj.chelan.raft

import akka.actor.FSM
import ericj.chelan.raft.messages._
import scala.concurrent.duration._
import ericj.chelan.raft.messages.RequestVoteResponse
import ericj.chelan.raft.messages.AppendEntriesRequest
import ericj.chelan.raft.messages.RequestVoteRequest

/**
 * This trait provides the FSM behaviours that participate in the Raft protocol.
 *
 * Created by Eric Jutrzenka on 06/03/2014.
 */
trait RaftBehaviour extends FSM[State, AllData] {

  /**
   * Returns a StateFunction that skips stale responses or else
   * handles the raft message.
   * @return
   */
  def raftReceive: StateFunction =
    dropStaleResponse orElse {
      case Event(m: RaftMessage, s: AllData) =>
        if (m.term > s.currentTerm) {
          goto(Follower) using handle(Event(m, s withNewTerm m.term))
        } else {
          stay() using handle(Event(m, s))
        }
    }

  /**
   * Performs the state transformation appropriate for the event.
   * @param event The event
   * @return The new state
   */
  private def handle(event: Event): AllData = {
    (handleRequestVoteRequest orElse
      handleRequestVoteResponse orElse
      handleAppendEntriesRequest orElse
      handleAppendEntriesResponse)(event)
  }

  /**
   * Drops a response where the term is less than the current term.
   * @return The state function which does this.
   */
  def dropStaleResponse: StateFunction = {
    case Event(m: RaftResponse, s) if m.term < s.currentTerm =>
      stay()
  }

  /**
   * Stand down for an AppendEntriesRPC in the current term. This is used
   * to stand down from Leader or Candidate.
   *
   * @return transition to follower and clear votedFor.
   */
  def standDown: StateFunction = {
    case Event(m: AppendEntriesRequest, s: AllData) if m.term == s.currentTerm =>
      goto(Follower) using handle(Event(m, s withNewTerm m.term))
  }

  /**
   * Sends out append entries to all followers on a heart beat.
   * @return
   */
  def appendEntries: StateFunction = {
    case Event(HeartBeat, s) =>
      s.electorate foreach {
        m => m.ref ! AppendEntriesRequest(s.currentTerm)
      }
      stay()
  }

  /**
   * Requests votes from all members on a heart beat.
   * @return
   */
  def requestVote: StateFunction = {
    case Event(HeartBeat, s) =>
      s.electorate foreach {
        m => m.ref ! RequestVoteRequest(s.currentTerm)
      }
      stay()
  }

  /**
   * Respond to a vote request. Only grant votes in the current term
   * and if not already voted.
   * @return stay in the current state and set votedFor.
   */
  def handleRequestVoteRequest: PartialFunction[Event, AllData] = {
    case Event(RequestVoteRequest(term), s: AllData) =>
      if (term < s.currentTerm || s.voted) {
        sender ! RequestVoteResponse(s.currentTerm, granted = false)
        s
      } else {
        sender ! RequestVoteResponse(s.currentTerm, granted = true)
        s.votedFor(sender())
      }
  }

  /**
   * Count a vote in the current term. And transition to Leader
   * if there are enough votes.
   *
   * The vote should be in the current term as UpdateTerm should have
   * updated the term if the term was greater and dropStaleResponse
   * should have removed it it was lesser.
   *
   * @return transition to leader or stay in candidate and record vote.
   */
  def handleRequestVoteResponse: PartialFunction[Event, AllData] = {
    case Event(RequestVoteResponse(term, granted), s) =>
      assert(term == s.currentTerm)
      s count Ballot(sender, granted)
  }

  /**
   * Performs validity check on the request and appends the entries
   * to the log. Rejects invalid requests.
   *
   * @return
   */
  def handleAppendEntriesRequest: PartialFunction[Event, AllData] = {
    case Event(AppendEntriesRequest(term, prevLogIndex, prevLogTerm, _, _), s) if term < s.currentTerm =>
      sender ! AppendEntriesResponse(s.currentTerm, success = false)
      s
    case Event(AppendEntriesRequest(term, prevLogIndex, prevLogTerm, entries, leaderCommit), s) =>
      assert(term == s.currentTerm)
      if (s.isValid(prevLogIndex, prevLogTerm, s.logVars)) {
        sender ! AppendEntriesResponse(s.currentTerm, success = true)
        s appendToLog entries
      } else {
        sender ! AppendEntriesResponse(s.currentTerm, success = false)
        s clearLogFrom prevLogIndex
      }

  }

  def handleAppendEntriesResponse: PartialFunction[Event, AllData] = {
    case Event(AppendEntriesResponse(term, success), s) if !success =>
      assert(term == s.currentTerm)

      s
  }

  /**
   * Start a new election term. Increment the term and send out vote requests.
   * @return transition to Candidate using the new term.
   */
  def startNewTerm: StateFunction = {
    case Event(ElectionTimeout, s: AllData) =>
      val newState: AllData = s.newTerm
      s.electorate foreach (m => m.ref ! RequestVoteRequest(newState.currentTerm))
      goto(Candidate) using newState
    case Event(StateTimeout, s: AllData) =>
      log.debug("Starting new term.")
      val newState: AllData = s.newTerm
      s.electorate foreach (m => m.ref ! RequestVoteRequest(newState.currentTerm))
      goto(Candidate) using newState
  }

  def electionTimeout(): FiniteDuration = (Math.random() * 150 milliseconds) + (150 milliseconds)
}
