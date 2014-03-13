package ericj.chelan.raft.messages

import akka.actor.ActorRef

case class Init(cluster: Array[ActorRef])
object HeartBeat
object ElectionTimeout

sealed trait RaftMessage{  val term: Int }
sealed trait RaftRequest extends RaftMessage
sealed trait RaftResponse extends RaftMessage

case class AppendEntriesRequest(term: Int) extends RaftRequest
case class AppendEntriesResponse(term: Int, success: Boolean) extends RaftResponse
case class RequestVoteRequest(term: Int) extends RaftRequest
case class RequestVoteResponse(term: Int, granted: Boolean) extends RaftResponse

