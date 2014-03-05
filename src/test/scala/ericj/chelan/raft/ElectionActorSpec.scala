package ericj.chelan.raft

import ericj.chelan.UnitSpec
import akka.testkit.{TestActorRef, TestProbe, TestFSMRef}
import ericj.chelan.raft.messages.{UpdateTerm, Vote, VoteRequest}
import akka.actor.{Actor, Props}
import scala.concurrent.duration._


/**
 * Created by ericj on 04/03/2014.
 */
class ElectionActorSpec extends UnitSpec {

  val electorate = Array(TestProbe(), TestProbe(), TestProbe(), TestProbe())

  val parent = system.actorOf(Props(new Actor {
    val child = context.actorOf(ElectionActor.props(electorate map { _.ref }))

    def receive = {
      case Elected => testActor forward Elected
      case x: UpdateTerm => testActor forward x
      case x => child forward x
    }

  }))

  "An ElectionActor" should "send out vote requests when it received NewElection" in {
    val electionActRef = TestActorRef(new ElectionActor(electorate map { _.ref }))
    electionActRef ! NewElection(5)
    electorate foreach { _.expectMsg(VoteRequest(5)) }
  }
  it should "notify the parent Elected when enough votes are received" in {
    parent ! NewElection(5)
    parent ! Vote(5, true)
    parent ! Vote(5, true)

    expectMsg(Elected)
  }
  it should  "not send Elected if not enough votes" in {
    parent ! NewElection(6)
    parent ! Vote(6, true)
    expectNoMsg(250 milliseconds)
  }
  it should  "not send Elected if votes are stale" in {
    parent ! NewElection(6)
    parent ! Vote(5, true)
    parent ! Vote(5, true)
    expectNoMsg(250 milliseconds)
  }
  it should  "not send Elected if votes are in newer term" in {
    parent ! NewElection(6)
    parent ! Vote(7, false)
    parent ! Vote(7, false)
    expectMsg(UpdateTerm(7))
    expectNoMsg(250 milliseconds)
  }
  it should "notify the parent UpdateTerm if vote is in newer term" in {
    parent ! NewElection(5)
    parent ! Vote(6, false)
    expectMsg(UpdateTerm(6))
  }

}
