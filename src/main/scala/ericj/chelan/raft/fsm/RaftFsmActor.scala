package ericj.chelan.raft.fsm

import akka.actor.{ActorRef, Actor, FSM}
import scala.concurrent.duration._

/**
 * Created by ericj on 23/02/2014.
 */
class RaftFsmActor extends Actor with FSM[State, Data] {

  val hartbeatTimeout = 300 milliseconds

  startWith(NotStarted, Uninitialized)

  when(NotStarted) {
    case Event(Init(members), Uninitialized) =>
      goto(Follower) using Generic(0, members, ActorRef.noSender)
  }

  when(Follower, stateTimeout = electionTimeout()) {
    case Event(AppendEntriesRpc(nTerm), s: Generic) =>
      if (nTerm >= s.term) {
        stay using s.withTerm(nTerm) replying AppendEntriesRpcResponse(nTerm, true)
      } else {
        stay using s replying AppendEntriesRpcResponse(s.term, false)
      }
    case Event(AppendEntriesRpc(nTerm), s: Generic) if nTerm >= s.term =>
      stay using s.withTerm(nTerm) replying AppendEntriesRpcResponse(nTerm, true)
    case Event(StateTimeout, Generic(term, members, votedFor)) =>
      val newTerm: Int = term + 1
      members.foreach( _ ! RequestVote(newTerm) )
      goto(Candidate) using Generic(newTerm, members, votedFor)
    case Event(RequestVote, Generic(term, members, votedFor))
      if votedFor != ActorRef.noSender =>
      stay using Generic(term, members, sender) replying Vote
  }

  when(Candidate, stateTimeout = electionTimeout()) {
    case Event(AppendEntriesRpc(nTerm), s: Generic) if nTerm > s.term =>
      goto(Follower) using s.withTerm(nTerm) replying AppendEntriesRpcResponse(nTerm, true)
    case Event(StateTimeout, Generic(term, members, votedFor)) =>
      val newTerm: Int = term + 1
      members.foreach( _ ! RequestVote(newTerm))
      stay using Generic(newTerm, members, votedFor)
  }

  when(Leader) {
    case Event(StateTimeout, s) => stay using s
    case Event(AppendEntriesRpc(nTerm), s: Generic) if nTerm > s.term =>
      goto(Follower) using s.withTerm(nTerm) replying AppendEntriesRpcResponse(nTerm, true)
  }

  def electionTimeout(): FiniteDuration = Math.random() * 300 milliseconds

}

case class Init(cluster: Array[ActorRef])
case class AppendEntriesRpc(term: Int)
case class AppendEntriesRpcResponse(term: Int, success: Boolean)
case class RequestVote(term: Int)
case class Vote(term: Int, granted: Boolean)

sealed trait State
case object NotStarted extends State
case object Leader extends State
case object Candidate extends State
case object Follower extends State

sealed trait Data
case object Uninitialized extends Data
case class Generic(term: Int,
                   cluster: Array[ActorRef],
                   votedFor: ActorRef) extends Data {

  def withTerm(newTerm: Int): Generic =  this.copy(term = newTerm)
}


