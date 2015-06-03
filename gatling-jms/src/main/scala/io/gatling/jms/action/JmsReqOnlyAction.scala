/**
 * Copyright 2011-2015 eBusiness Information, Groupe Excilys (www.ebusinessinformation.fr)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gatling.jms.action

import java.util.concurrent.atomic.AtomicBoolean
import javax.jms.Message

import io.gatling.jms.JmsAction

import scala.util.control.NonFatal

import io.gatling.core.action.{ Failable, Interruptable }
import io.gatling.core.session.{ Expression, Session }
import io.gatling.core.stats.StatsEngine
import io.gatling.core.util.TimeHelper.nowMillis
import io.gatling.core.validation.Validation
import io.gatling.core.session.Session
import io.gatling.core.validation.{ SuccessWrapper, Validation }
import io.gatling.jms.client.JmsClient
import io.gatling.jms.protocol.JmsProtocol
import io.gatling.jms.request._

import akka.actor.{ ActorRef, Props }

object JmsReqOnlyAction {
  val BlockingReceiveReturnedNull = new Exception("Blocking receive returned null. Possibly the consumer was closed.")

  def props(attributes: JmsAttributes, protocol: JmsProtocol, tracker: ActorRef, statsEngine: StatsEngine, next: ActorRef) =
    Props(new JmsReqOnlyAction(attributes, protocol, tracker, statsEngine, next))
}

/**
 * Core JMS Action to handle Request semantics
 * <p>
 * This handles the core "send"ing of messages. Gatling calls the execute method to trigger a send.
 * This implementation then forwards it on to a tracking actor.
 */
class JmsReqOnlyAction(attributes: JmsAttributes, protocol: JmsProtocol, tracker: ActorRef, val statsEngine: StatsEngine, val next: ActorRef)
    extends Interruptable with Failable with JmsAction {

  // Create a client to refer to
  val client = JmsClient(protocol, attributes.destination, attributes.replyDestination)
  val messageMatcher = protocol.messageMatcher

  override def postStop(): Unit = {
    client.close()
  }

  /**
   * Framework calls the execute() method to send a single request
   * <p>
   * Note this does not catch any exceptions (even JMSException) as generally these indicate a
   * configuration failure that is unlikely to be addressed by retrying with another message
   */
  def executeOrFail(session: Session): Validation[Unit] = {

    // send the message
    val start = nowMillis

    val msg = resolveProperties(attributes.messageProperties, session).flatMap { messageProperties =>
      attributes.message match {
        case BytesJmsMessage(bytes) => bytes(session).map(bytes => client.sendBytesMessage(bytes, messageProperties))
        case MapJmsMessage(map)     => map(session).map(map => client.sendMapMessage(map, messageProperties))
        case ObjectJmsMessage(o)    => o(session).map(o => client.sendObjectMessage(o, messageProperties))
        case TextJmsMessage(txt)    => txt(session).map(txt => client.sendTextMessage(txt, messageProperties))
      }
    }

    msg.map { msg =>
      // notify the tracker that a message was sent
      tracker ! MessageSent(messageMatcher.requestID(msg), start, nowMillis, attributes.checks, session, next, attributes.requestName)
      tracker ! MessageReceived(messageMatcher.requestID(msg), nowMillis, msg)
      logMessage(s"Message sent ${msg.getJMSMessageID}", msg)
      logMessage(s"Message received ${msg.getJMSMessageID}", msg)
    }
  }

}
