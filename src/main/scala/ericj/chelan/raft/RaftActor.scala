package ericj.chelan.raft

import akka.actor.{Actor, LoggingFSM, ActorRef, FSM}
import ericj.chelan.raft.messages._
import scala.concurrent.duration._

/**
 * Created by ericj on 03/03/2014.
 */
class RaftActor extends Actor with LoggingFSM[State, AllData] {

  val initialState = AllData(ReplicaVars(0, None), null)

  startWith(NotStarted, initialState)

  when(NotStarted) {
    case Event(Init(members), s) =>
      goto(Follower) using s.copy(elector =
        context.actorOf(ElectionActor.props(members), "delegate"))
  }

  when(Follower, stateTimeout = electionTimeout()) {
    case Event(StateTimeout, s: AllData) =>
      goto(Candidate) using s.newTerm()
    case Event(VoteRequest(term), s: AllData) =>
      if (term < s.currentTerm || s.voted) {
        sender ! Vote(s.currentTerm, false)
        stay
      } else {
        sender ! Vote(s.currentTerm, true)
        stay using s.votedFor(sender)
      }
    case Event(AppendEntriesRpc(term), s) =>
      stay using s
  }

  when(Candidate, stateTimeout = electionTimeout()) {
    case Event(StateTimeout, s: AllData) => stay using s.newTerm
    case Event(Elected, s) => goto(Leader) using s
    case Event(UpdateTerm(newTerm), s: AllData) if newTerm >= s.currentTerm =>
      goto(Follower) using s.copy(replicaVars = ReplicaVars(newTerm, None))
  }

  when(Leader)(FSM.NullFunction)

  whenUnhandled {
    case Event(UpdateTerm(newTerm), s: AllData) if (newTerm > s.currentTerm) =>
      goto(Follower) using s.copy(replicaVars = ReplicaVars(newTerm, None))
    case Event(UpdateTerm(newTerm), s: AllData) if (newTerm == s.currentTerm) =>
      stay
    case Event(VoteRequest(_), s) =>
      stay replying(Vote(s.replicaVars.currentTerm, false))
    case Event(Elected, s) =>
      stay
  }

  onTransition {
    case _ -> Candidate =>
      nextStateData.elector ! NewElection(nextStateData.replicaVars.currentTerm)
    case _ -> Leader =>
      log.info(s"Elected as leader in term: ${nextStateData.currentTerm}")
      nextStateData.elector ! AppendEntriesRpc(nextStateData.currentTerm)
      context.parent ! NewLeader(nextStateData.currentTerm)
  }

  def electionTimeout(): FiniteDuration = (Math.random() * 150 milliseconds) + (150 milliseconds)

}

sealed trait Data

case class AllData(replicaVars: ReplicaVars, elector: ActorRef) extends Data {
  def newTerm(newTerm: Int): AllData = {
    assert(newTerm > replicaVars.currentTerm)
    copy(replicaVars = ReplicaVars(newTerm, None))
  }

  def newTerm(): AllData = newTerm(replicaVars.currentTerm + 1)

  def votedFor(candidate: ActorRef) = copy(replicaVars = replicaVars.copy(votedFor = Some(candidate)))
  def voted: Boolean = replicaVars.votedFor != None
  def currentTerm = replicaVars.currentTerm
}

case class ReplicaVars(currentTerm: Int, votedFor: Option[ActorRef])

sealed trait State

case object Follower extends State

case object NotStarted extends State

case object Candidate extends State

case object Leader extends State

