package ericj.chelan.raft

import akka.actor.{Props, ActorRef, Actor}
import ericj.chelan.raft.messages.{AppendEntriesRpc, UpdateTerm, Vote, VoteRequest}
import scala.concurrent.duration._

/**
 * Carries out an election on behalf of a candidate.
 *
 * Created by ericj on 24/02/2014.
 */
class ElectionActor(electorate: Array[ActorRef]) extends Actor {

  import context._

  override def receive: Receive = waitForNewElection

  def countVotes(votes: Int, currentTerm: Int): Receive  = {
    case Vote(term, granted) if (granted && term == currentTerm) =>
      if (enoughVotes(votes + 1)) {
        parent ! Elected
        become(waitForNewElection)
      }
      else become(activeElection(votes + 1, currentTerm))
    case Vote(term, granted) if (term > currentTerm) =>
      parent ! UpdateTerm(term)
      become(waitForNewElection)
  }

  def waitForNewElection: Receive = {
    case NewElection(term: Int) =>
      electorate foreach ( _ ! VoteRequest(term) )
      become(activeElection(0, term))
    case m: AppendEntriesRpc =>
      electorate foreach {
        ref => system.scheduler.schedule(0 seconds, 100 millis, ref, m)
      }
  }

  def activeElection(votes: Int, currentTerm: Int) =
    waitForNewElection orElse countVotes(votes, currentTerm)

  def enoughVotes(votes: Int): Boolean = {
    Math.floor(electorate.length / 2.0) <=  votes
  }
}

object ElectionActor {
  /**
   * Create Props for an actor of this type.
   * @param electorate The electorate to be passed to this actor’s constructor.
   * @return a Props for creating this actor, which can then be further configured
   *         (e.g. calling `.withDispatcher()` on it)
   */
  def props(electorate: Array[ActorRef]): Props =
    Props(classOf[ElectionActor], electorate)
}


/**
 * Used by both the candidate and elector to start a new ballot.
 *
 * @param term the term of the new election.
 */
case class NewElection(term: Int)

case object Elected
