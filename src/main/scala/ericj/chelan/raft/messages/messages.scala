package ericj.chelan.raft.messages

import akka.actor.ActorRef

sealed trait RaftMessage{  val term: Int }

case class Init(cluster: Array[ActorRef])

case class AppendEntriesRpc(term: Int) extends RaftMessage

case class AppendEntriesRpcResponse(term: Int, success: Boolean) extends RaftMessage

case class VoteRequest(term: Int) extends RaftMessage

case class Vote(term: Int, granted: Boolean) extends RaftResponse

case class UpdateTerm(newTerm: Int)

sealed trait RaftResponse { val term: Int }

case class AppendRequestResponse(term: Int) extends RaftResponse

sealed trait MonitoringMessage

case class NewLeader(term: Int) extends MonitoringMessage