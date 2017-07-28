/*
 * Copyright (C) 2016 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.khermes.cluster.supervisor

import java.util.UUID

import akka.Done
import akka.actor.{Actor, ActorLogging}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import akka.stream._
import com.stratio.khermes.cluster.supervisor.NodeSupervisorActor.Result
import com.stratio.khermes.commons.config.AppConfig
import com.stratio.khermes.flow.FlowCreator
import com.stratio.khermes.helpers.faker.Faker
import com.stratio.khermes.metrics.KhermesMetrics
import com.typesafe.config.Config

import scala.concurrent.Future
import scala.util.Try

/**
 * Supervisor that will manage a thread that will generate data along the cluster.
 * @param config with all needed configuration.
 */
class NodeSupervisorActor(implicit config: Config) extends Actor with ActorLogging with KhermesMetrics {

  import DistributedPubSubMediator.Subscribe

  private val mediator = DistributedPubSub(context.system).mediator
  mediator ! Subscribe("content", self)

  private var khermesExecutor: Option[(KillSwitch, Future[Done])] = None
  private val id = UUID.randomUUID.toString

  private val khermes = Faker(Try(config.getString("khermes.i18n")).toOption.getOrElse("EN"),
    Try(config.getString("khermes.strategy")).toOption)

  private implicit val materializer = ActorMaterializer()

  override def receive: Receive = {
    case NodeSupervisorActor.Start(ids, hc) =>
      log.debug("Received start message")

      execute(ids, () => {

        val flow = FlowCreator.create(hc)
        khermesExecutor.foreach(_._1.shutdown())
        khermesExecutor = Option(flow.run())
      })

    case NodeSupervisorActor.Stop(ids) =>
      log.debug("Received stop message")
      execute(ids, () => {
        khermesExecutor.foreach(_._1.shutdown())
        khermesExecutor = None
      })

    case NodeSupervisorActor.List(ids, commandId) =>
      log.debug("Received list message")
      execute(ids, () => {
        val status = khermesExecutor.map(!_._2.isCompleted)
        sender ! Result(s"$id | $status", commandId)
        //context.system.eventStream.publish(s"$id | $status")
      })
  }

  protected[this] def isAMessageForMe(ids: Seq[String]): Boolean = ids.contains(id)

  protected[this] def execute(ids: Seq[String], callback: () => Unit): Unit =
    if (ids.nonEmpty && !isAMessageForMe(ids)) log.debug("Is not a message for me!") else callback()
}




object NodeSupervisorActor {

  case class Start(workerIds: Seq[String], khermesConfig: AppConfig)

  case class Stop(workerIds: Seq[String])

  case class List(workerIds: Seq[String], commandId: String)

  case class Result(value: String, commandId: String)

  object WorkerStatus extends Enumeration {
    val Started, Stopped = Value
  }

}
