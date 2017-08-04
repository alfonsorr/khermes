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

package com.stratio.khermes.utils

import java.io.File
import java.net.ServerSocket
import java.util.Properties

import com.stratio.khermes.persistence.kafka.KafkaClient
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{SystemTime, TestUtils}
import org.apache.curator.test.TestingServer
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.rules.TemporaryFolder

import scala.util.{Failure, Try}

trait EmbeddedServersUtils extends LazyLogging {
  type TopicName = String
  val zookeeperConnectString = "127.0.0.1:2181"
  val tmpFolder = new TemporaryFolder()
  tmpFolder.create()
  val logDir = tmpFolder.newFolder("kafkatest")
  val loggingEnabled = true

  def withEmbeddedKafkaServer(topicsToBeCreated: Seq[TopicName])(function: KafkaServer => Any): Unit = {
    withEmbeddedZookeeper() { zookeeperServer =>
      zookeeperServer.start
      val kafkaConfig = new KafkaConfig(kafkaConfiguration(logDir, zookeeperServer.getConnectString), loggingEnabled)

      logger.debug("Starting embedded Kafka broker (with log.dirs={} and ZK ensemble at {}) ...",
        logDir, zookeeperConnectString)

      val kafkaServer = TestUtils.createServer(kafkaConfig, SystemTime)
      Try {
        kafkaServer.startup
        val brokerList =
          s"""${kafkaServer.config.hostName}:${
            Integer.toString(kafkaServer.boundPort(SecurityProtocol.PLAINTEXT))
          }"""

        logger.debug("Startup of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
          brokerList, zookeeperConnectString)

        function(kafkaServer)
      }.recoverWith{case a => logger.error("embedded kafka coudn't start",a); a.printStackTrace(); Failure(a) }
      kafkaServer.shutdown
      zookeeperServer.stop
    }
  }


  def withEmbeddedZookeeper()(function: TestingServer => Any): Unit = {
    function(new TestingServer(-1))
  }

  def withKafkaProducer[V](kafkaServer: KafkaServer)(testFunction: KafkaProducer[String, V] => Any): Unit = {
    val props = kafkaServer.config.originals
    val producer: KafkaProducer[String, V] = new KafkaProducer(props)
    testFunction(producer)
  }


  def withKafkaConsumer(kafkaServer: KafkaServer)(testFunction: KafkaConsumer[String,String] => Any): Unit = {
    val props = kafkaServer.config.originals
    val config = new Properties()
    config.put("group.id", "foo")
    config.put("bootstrap.servers", props.get("kafka.bootstrap.servers"))
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String,String](config, new StringDeserializer(), new StringDeserializer())
    testFunction(consumer)
    consumer.close()
  }

  def withKafkaClient[V](kafkaServer: KafkaServer)(function: KafkaClient[V] => Any): Unit = {
    val kafkaClient = new KafkaClient[V](ConfigFactory.parseMap(kafkaServer.config.originals))
    function(kafkaClient)
  }

  //TODO: Accept initial config parameter (specific traits)
  private def kafkaConfiguration(logDir: File, zkConnectString: String) = {
    val kafkaConfig = new Properties()
    val randomPort = getRandomPort.toString
    kafkaConfig.put(KafkaConfig.ZkConnectProp, zkConnectString)
    kafkaConfig.put(KafkaConfig.BrokerIdProp, "0")
    kafkaConfig.put(KafkaConfig.HostNameProp, "127.0.0.1")
    kafkaConfig.put(KafkaConfig.PortProp, randomPort)
    kafkaConfig.put(KafkaConfig.NumPartitionsProp, "1")
    kafkaConfig.put(KafkaConfig.AutoCreateTopicsEnableProp, "true")
    kafkaConfig.put(KafkaConfig.MessageMaxBytesProp, "1000000")
    kafkaConfig.put(KafkaConfig.ControlledShutdownEnableProp, "true")
    kafkaConfig.put("kafka.bootstrap.servers", "127.0.0.1:" + randomPort)
    kafkaConfig.put("kafka.key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaConfig.put("kafka.value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaConfig.setProperty(KafkaConfig.LogDirProp, logDir.getAbsolutePath)
    //effectiveConfig.putAll(initialConfig);
    kafkaConfig
  }

  private def openSocket: ServerSocket = Try {
    new ServerSocket(0)
  }.recoverWith { case _: Throwable => Try(openSocket) }.get

  private def closeSocket(socket: ServerSocket): Unit = Try {
    socket.close()
  }.recoverWith { case _: Throwable => Try(closeSocket(socket)) }

  private def getRandomPort: Int = {
    val socket = openSocket
    val port = socket.getLocalPort
    closeSocket(socket)
    port
  }

}
