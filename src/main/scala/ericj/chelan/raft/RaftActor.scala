package ericj.chelan.raft

import ericj.chelan.raft.messages._
import ericj.chelan.raft.messages.AppendEntriesResponse
import ericj.chelan.raft.messages.RequestVoteRequest
import ericj.chelan.raft.messages.AppendEntriesRequest
import akka.persistence.EventsourcedProcessor
import akka.actor.{ Actor, ReceiveTimeout, Cancellable, Props }

import scala.concurrent.duration._
import scala.util.Random

sealed trait ElectionEvent

case object StoodDown

/**
 *
 * Created by Eric Jutrzenka on 20/03/2014.
 */
class RaftActor(val id: String = "my-stable-processor-id") extends EventsourcedProcessor with FollowerBehaviour with CandidateBheaviour with LeaderBehaviour {

  import context._

  override def processorId = id

  var stateData: RaftState = RaftState()

  var sate: State = NotStarted

  var heartBeat: Cancellable = new Cancellable {
    override def cancel(): Boolean = false
    override def isCancelled: Boolean = true
  }

  val receiveCommand: Receive = NotStarted.behaviour

  val receiveRecover: Receive = updateState

  def goto(newState: State) {
    if (sate != newState) {
      become(handleStaleRequest orElse newState.behaviour)
      (sate, newState) match {
        case (_, Follower) =>
          cancelHeartBeat(); setReceiveTimeout((Random.nextInt(150) millis) + (100 millis))
        case (Candidate, Leader) =>
          startHeartBeat(); setReceiveTimeout(Duration.Undefined)
        case (_, _) => ()
      }
    } else ()
    sate = newState
  }

  private def cancelHeartBeat() {
    heartBeat.cancel()
  }

  private def startHeartBeat() {
    heartBeat.cancel()
    heartBeat = system.scheduler.schedule(0 millis, 100 millis, self, HeartBeat)
  }

  private def updateTerm: Receive = {
    case m: RaftRequest if m.term > stateData.currentTerm =>
      persist(NewTerm(m.term)) {
        evt =>
          updateState(evt)
          self forward m
      }
  }

  sealed trait State {
    val behaviour: Receive
  }

  case object NotStarted extends State {
    val behaviour: Receive = {
      case Init(members) =>
        persist(Initialised(members)) {
          evt =>
            updateState(evt)
            goto(Follower)
        }
    }
  }

  case object Follower extends State {
    val behaviour = updateTerm orElse handleElectionTimeout orElse followerBheaviour
  }

  case object Candidate extends State {
    val behaviour: Receive = handleElectionTimeout orElse candidateBheaviour

  }

  case object Leader extends State {
    val behaviour: Receive = leaderBeahviour
  }

  def updateState: Receive = {
    case evt: StateEvent => {
      stateData = stateData updated evt
      system.eventStream.publish(evt)
    }
  }

  private def handleElectionTimeout: Receive = {
    case ReceiveTimeout =>
      persist(TermIncremented) {
        evt =>
          updateState(evt)
          stateData.electorate foreach (m => m.ref ! RequestVoteRequest(stateData.currentTerm))
          goto(Candidate)
      }
  }

  def standDownIfMessageTerm(f: Int => Boolean): Receive = {
    case m: RaftMessage if f(m.term) =>
      self forward m
      goto(Follower)
  }

  private def handleStaleRequest: Receive = {
    case m: AppendEntriesRequest if m.term < stateData.currentTerm =>
      sender ! AppendEntriesResponse(stateData.currentTerm, None)
    case m: RequestVoteRequest if m.term < stateData.currentTerm =>
      sender ! RequestVoteResponse(stateData.currentTerm, false)
  }

}

object RaftActor {
  def props(id: String): Props = Props(new RaftActor(id))
}

trait LeaderBehaviour {
  this: RaftActor =>
  val leaderBeahviour: Receive = {
    standDownIfMessageTerm(_ > stateData.currentTerm) orElse
      appendEntries orElse
      handleAppendEntriesResponse orElse
      handleClientRequest
  }

  private def appendEntries: Receive = {
    case HeartBeat =>
      stateData.electorate.foreach {
        m =>
          m.ref ! AppendEntriesRequest(
            stateData.currentTerm,
            prevLogIndex = m.nextIndex - 1,
            prevLogTerm = stateData termOfLogEntryAtIndex (m.nextIndex - 1),
            entries = stateData.logVars.log.filter(p => p.index >= m.nextIndex) map (e => NewEntry(e.term, e.value)) reverse,
            leaderCommit = 0
          )
      }
  }

  private def handleAppendEntriesResponse: Receive = {
    case AppendEntriesResponse(term, Some(lastAppliedIndex)) if term == stateData.currentTerm =>
      updateState(AppendAccepted(sender(), lastAppliedIndex))
    case AppendEntriesResponse(term, None) if term == stateData.currentTerm =>
      updateState(AppendRejected(sender()))
  }

  private def handleClientRequest: Receive = {
    case ClientRequest(payload) =>
      persist(AppendedToLog(List(NewEntry(stateData.currentTerm, payload))))(updateState)
  }
}

trait CandidateBheaviour {
  this: RaftActor =>
  val candidateBheaviour: Receive = {
    handleClientRequest orElse
      handleRequestVoteResponse orElse
      standDownIfMessageTerm(_ >= stateData.currentTerm)
  }

  private def handleClientRequest: Receive = {
    case m: ClientRequest =>
      self forward m
  }

  private def handleRequestVoteResponse: Receive = {
    case RequestVoteResponse(term, granted) if term == stateData.currentTerm =>
      updateState(BallotCounted(sender(), granted))
      if (stateData.enoughVotes) {
        updateState(Elected)
        goto(Leader)
      } else ()
  }
}

trait FollowerBehaviour {
  this: RaftActor =>
  val followerBheaviour: Receive =
    handleRequestVoteRequest orElse
      handleAppendEntriesRequest orElse
      forwardClientRequest

  private def handleAppendEntriesRequest: Receive = {
    case AppendEntriesRequest(term, prevLogIndex, prevLogTerm, entries, leaderCommit) =>
      assert(term == stateData.currentTerm)
      if (stateData.leader != Some(sender()))
        updateState(NewLeaderDetected(sender()))
      if (stateData.isValid(prevLogIndex, prevLogTerm)) {
        if (!entries.isEmpty) {
          persist(AppendedToLog(entries)) {
            evt =>
              updateState(evt)
              sendSuccesfulAppendEntriesResponse()
          }
        } else {
          sendSuccesfulAppendEntriesResponse()
        }
      } else {
        if (prevLogIndex <= stateData.logVars.log.head.index) {
          persist(LogClearedFrom(prevLogIndex)) {
            evt =>
              updateState(evt)
              sendFailedAppendEntriesResponse()
          }
        } else {
          sendFailedAppendEntriesResponse()
        }
      }

  }

  private def sendSuccesfulAppendEntriesResponse() {
    sender ! AppendEntriesResponse(stateData.currentTerm, Some(stateData.logVars.log.head.index))
  }

  private def sendFailedAppendEntriesResponse() {
    sender ! AppendEntriesResponse(stateData.currentTerm, None)
  }

  /**
   * Respond to a vote request. Only grant votes in the current term
   * and if not already voted.
   * @return stay in the current state and set votedFor.
   */
  private def handleRequestVoteRequest: Receive = {
    case RequestVoteRequest(term, lastLogIndex, lastLogTerm) =>
      assert(term == stateData.currentTerm)
      if (stateData.voted ||
        lastLogTerm < stateData.logVars.log.head.term ||
        (lastLogTerm == stateData.logVars.log.head.term && lastLogIndex < stateData.logVars.log.head.index)) {
        sender ! RequestVoteResponse(stateData.currentTerm, success = false)
      } else {
        persist(VotedFor(sender())) {
          evt =>
            updateState(evt)
            sender ! RequestVoteResponse(stateData.currentTerm, success = true)
        }
      }
  }

  private def forwardClientRequest: Receive = {
    case m: ClientRequest =>
      stateData.leader.get forward m
  }
}

