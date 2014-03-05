package ericj.chelan.raft

import akka.actor.{ActorRef, Props, Actor}
import ericj.chelan.raft.messages.{Init, RaftResponse, RaftMessage, UpdateTerm}

/**
 * Created by ericj on 03/03/2014.
 */
class RaftProcessor(val initialTerm: Int, val target: ActorRef) extends Actor {

  import context._

  override def receive: Actor.Receive = handleWithTerm(initialTerm)

  // vote request can cause an update term here
  // which will cause the raftactor to go out of
  // candidate state.
  def handleWithTerm(term: Int): Receive = {
    case m: RaftMessage =>
      if (m.term > term) {
        target ! UpdateTerm(m.term)
        become(handleWithTerm(m.term))
      }
      target forward m
    case m: RaftResponse if m.term == term =>
      target forward m
  }
}

object RaftProcessor {
  /**
   * Create Props for an actor of this type.
   * @param initialTerm The inital term to be passed to this actor’s constructor.
   * @return a Props for creating this actor, which can then be further configured
   *         (e.g. calling `.withDispatcher()` on it)
   */
  def props(initialTerm: Int, target: ActorRef): Props =
    Props(classOf[RaftProcessor], initialTerm, target)
}
