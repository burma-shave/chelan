package ericj.chelan.raft

import akka.actor.{FSM, Actor, LoggingFSM, ActorRef}
import ericj.chelan.raft.messages._
import scala.concurrent.duration._

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
        electorate = members)
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
      case a@FSM.State(state, s: AllData, timeout, stopReason, replies) if (s.enoughVotes()) =>
        goto(Leader)
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

case class AllData(replicaVars: ReplicaVars, electorate: Array[ActorRef], ballots: List[Ballot] = List.empty)
  extends Data {
  def newTerm(newTerm: Int): AllData = {
    assert(newTerm >= replicaVars.currentTerm)
    copy(replicaVars = ReplicaVars(newTerm, None), ballots = List.empty)
  }

  def newTerm(): AllData = newTerm(replicaVars.currentTerm + 1)

  def votedFor(candidate: ActorRef) = copy(replicaVars = replicaVars.copy(votedFor = Some(candidate)))

  def voted: Boolean = replicaVars.votedFor != None

  def currentTerm = replicaVars.currentTerm

  def enoughVotes(): Boolean = {
    ballots.count(b => b.granted) >= electorate.length / 2
  }

  def countBallot(ballot: Ballot) =
    if (!counted(ballot.elector)) copy(ballots = ballots.+:(ballot))
    else this

  def counted(elector: ActorRef) = ballots.filter(b => b.elector == elector).nonEmpty
}

case class Ballot(elector: ActorRef, granted: Boolean)

case class ReplicaVars(currentTerm: Int, votedFor: Option[ActorRef])

sealed trait State

case object Follower extends State

case object NotStarted extends State

case object Candidate extends State

case object Leader extends State

