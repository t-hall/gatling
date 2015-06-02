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
package io.gatling.jms

import javax.jms.Message

import com.typesafe.scalalogging.LazyLogging
import io.gatling.core.session.{ Expression, Session }
import io.gatling.core.validation.{ SuccessWrapper, Validation }
/**
 * Created by thall on 5/7/15.
 */
trait JmsAction extends LazyLogging {

  def resolveProperties(properties: Map[Expression[String], Expression[Any]],
                        session: Session): Validation[Map[String, Any]] = {
    properties.foldLeft(Map.empty[String, Any].success) {
      case (resolvedProperties, (key, value)) =>
        val newProperty: Validation[(String, Any)] =
          for {
            key <- key(session)
            value <- value(session)
          } yield key -> value

        for {
          newProperty <- newProperty
          resolvedProperties <- resolvedProperties
        } yield resolvedProperties + newProperty
    }
  }

  def logMessage(text: String, msg: Message): Unit = {
    logger.debug(text)
    logger.trace(msg.toString)
  }
}
