package ericj.chelan.raft

import ericj.chelan.UnitSpec
import akka.testkit.{DefaultTimeout, TestKit, TestActorRef}
import ericj.chelan.raft.messages._
import akka.actor._
import org.scalatest._
import ericj.chelan.raft.messages.AppendEntriesRpc
import ericj.chelan.raft.messages.UpdateTerm

/**
 * Created by ericj on 03/03/2014.
 */
class RaftProcessorSpec(_system: ActorSystem) extends TestKit(_system) with DefaultTimeout with FlatSpecLike with Matchers with
OptionValues with Inside with Inspectors with BeforeAndAfterEach with BeforeAndAfterAll {
  def this() = this(ActorSystem("MySpec"))

  val initialTerm = 10

  var aut: ActorRef = system.actorOf(RaftProcessor.props(initialTerm, testActor))

  "A processor" should "send UpdateTerm if the message contains a newer term" in {
    aut ! AppendEntriesRpc(initialTerm + 1)
    expectMsg(UpdateTerm(initialTerm + 1))
    expectMsg(AppendEntriesRpc(initialTerm + 1))
  }
  it should "not send UpdateTerm if the term is current" in {
    aut ! AppendEntriesRpc(initialTerm)
    expectMsg(AppendEntriesRpc(initialTerm ))
  }
  it should "discard stale responses" in  {
    aut ! Vote(initialTerm - 1, false)
  }
  it should "forward current responses" in  {
    aut ! Vote(initialTerm, false)
    expectMsg(Vote(initialTerm, false))
  }


  override protected def afterEach(): Unit = {
    system.stop(aut)
    aut = system.actorOf(RaftProcessor.props(initialTerm, testActor))
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }


}

case class ExternalRequest(payload: RaftMessage)
case class ExternalResponse(payload: RaftResponse)

