package com.stratio.khermes.flow

import java.time.LocalTime
import java.util.UUID

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.stratio.khermes.cluster.BaseActorTest
import com.stratio.khermes.commons.config.AppConfig
import com.stratio.khermes.utils.EmbeddedServersUtils
import kafka.server.KafkaConfig

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future}

class FlowCreatorTest
  extends BaseActorTest
  with EmbeddedServersUtils{

  implicit val materializer = ActorMaterializer()


  val Topic = s"topic-${UUID.randomUUID().toString}"

  private def kafkaConfigContent(kafkaConfig:KafkaConfig) =
    s"""
      |kafka {
      |  bootstrap.servers = "${kafkaConfig.advertisedHostName}:${kafkaConfig.advertisedPort}"
      |  acks = "-1"
      |  key.serializer = "org.apache.kafka.common.serialization.StringSerializer"
      |  value.serializer = "org.apache.kafka.common.serialization.StringSerializer"
      |}
    """.stripMargin

  private val khermesConfigContentUnbounded =
    """
      |khermes {
      |  templates-path = "/tmp/khermes/templates"
      |  topic = "chustas"
      |  template-name = "chustasTemplate"
      |  i18n = "ES"
      |}
    """.stripMargin

  private def khermesConfigContentLimited(events:Int, time:FiniteDuration) =
    s"""
      |khermes {
      |  templates-path = "/tmp/khermes/templates"
      |  topic = "chustas"
      |  template-name = "chustasTemplate"
      |  i18n = "ES"
      |  timeout-rules {
      |    number-of-events: $events
      |    duration: ${time.toSeconds} seconds
      |  }
      |}
    """.stripMargin

  private val textToReturn =
    """{
      |  "name" : "Something"
      |}""".stripMargin


  private val templateContentNoVariables =
    s"""@import com.stratio.khermes.helpers.faker.Faker
      |@(faker: Faker)$textToReturn""".stripMargin

  private val templateContent =
    """
      |@import com.stratio.khermes.helpers.faker.Faker
      |
      |@(faker: Faker)
      |{
      |  "name" : "@(faker.Name.firstName)"
      |}
    """.stripMargin

  type testResult = (Int,LocalTime)

  import scala.concurrent.duration._

  "A FlowCreator" should {

    "have a flow that creates strings from templates" in {

      val events = 10
      val eventsPeriod = 1.seconds

      class FlowCreatorTester extends FlowCreator[Future[String]] {
        override private[flow] def createSink(hc: AppConfig) = {
          Sink.head[String]
        }
      }

      val hc = AppConfig(khermesConfigContentLimited(events, eventsPeriod), "", templateContentNoVariables)
      val testFlow = new FlowCreatorTester().create(hc)
      val (_, probe) = testFlow.run()

      val results = Await.result(probe, eventsPeriod)

      results should be(textToReturn)
    }

    "be limited to the expected amount of messages per second" in {
      val events = 10000
      val eventsPeriod = 1.seconds
      val testTimeInSeconds = 10.seconds
      val aceptedError = 100.millis

      class FlowCreatorTester extends FlowCreator[Future[Seq[testResult]]] {
        override private[flow] def createSink(hc: AppConfig) = {
          val events = hc.timeoutNumberOfEventsOption.get
          Flow[String].scan(0)((counter, _) => counter + 1).filter(n => n % events == 0).map(n => (n, LocalTime.now())).toMat(Sink.seq)(Keep.right)
        }
      }

      val hc = AppConfig(khermesConfigContentLimited(events, eventsPeriod), "", templateContent)
      val testFlow = new FlowCreatorTester().create(hc)
      val (killSwitch, probe) = testFlow.run()

      Thread.sleep(testTimeInSeconds.toMillis)
      killSwitch.shutdown()
      val results = Await.result(probe, 1.seconds).map(_._2).drop(2) //the first two seconds the speed is not optimal

      results.zip(results.tail).map(r => eventsPeriod - (r._2.toNanoOfDay - r._1.toNanoOfDay).nanos).foreach(errorTime => {
        errorTime should be > (-aceptedError)
        errorTime should be < aceptedError
      })
    }

    "create an unbounded flow" in {
      val testTimeInSeconds = 3.seconds
      val aceptedMin = 10000l

      class FlowCreatorTester extends FlowCreator[Future[Long]] {
        override private[flow] def createSink(hc: AppConfig) = {
          Sink.fold(0l)((counter, _) => counter + 1)
        }
      }

      val hc = AppConfig(khermesConfigContentUnbounded, "", templateContent)
      val testFlow = new FlowCreatorTester().create(hc)
      val (killSwitch, probe) = testFlow.run()

      Thread.sleep(testTimeInSeconds.toMillis)
      killSwitch.shutdown()
      val results = Await.result(probe, 1.seconds)

      results should be > aceptedMin
      println(results)

    }



    "send to kafka" in {
      withEmbeddedKafkaServer(Seq(Topic)) { kafkaServer =>
        withKafkaConsumer(kafkaServer){ consumer =>
          import scala.collection.JavaConverters._

          consumer.subscribe(Seq("chustas").asJava)
          val testTimeInSeconds = 3.seconds
          val events = 10

          val hc = AppConfig(khermesConfigContentLimited(events, 1.seconds), kafkaConfigContent(kafkaServer.config), templateContentNoVariables)
          val testFlow = FlowCreator.create(hc)
          val (killSwitch, probe) = testFlow.run()

          Thread.sleep(testTimeInSeconds.toMillis)
          killSwitch.shutdown()
          val _ = Await.result(probe, 1.seconds)

          val scalaResult = consumer.poll(1.seconds.toMillis).iterator().asScala.toList

          scalaResult should not be empty
          scalaResult.foreach(a => a.value() should be(textToReturn))
          println(scalaResult.length)
        }
      }
    }
  }
}
