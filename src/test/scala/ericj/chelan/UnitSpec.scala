package ericj.chelan

import org.scalatest._
import akka.testkit.{DefaultTimeout, TestKit}
import akka.actor.ActorSystem

/**
 * Created by ericj on 13/02/2014.
 */
abstract class UnitSpec extends TestKit(ActorSystem("test"))
with DefaultTimeout with FlatSpecLike with Matchers with
OptionValues with Inside with Inspectors
