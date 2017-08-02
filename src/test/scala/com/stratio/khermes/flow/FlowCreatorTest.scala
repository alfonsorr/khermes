package com.stratio.khermes.flow

import java.time.LocalTime

import akka.Done
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.stream.testkit.TestSubscriber.Probe
import akka.stream.testkit.scaladsl.TestSink
import com.stratio.khermes.cluster.BaseActorTest
import com.stratio.khermes.commons.config.AppConfig

import scala.concurrent.{Await, Future}

class FlowCreatorTest extends BaseActorTest{

  implicit val materializer = ActorMaterializer()

  val kafkaConfigContent =
    """
      |kafka {
      |  bootstrap.servers = "localhost:9092"
      |  acks = "-1"
      |  key.serializer = "org.apache.kafka.common.serialization.StringSerializer"
      |  value.serializer = "org.apache.kafka.common.serialization.StringSerializer"
      |}
    """.stripMargin

  val khermesConfigContentUnbounded =
    """
      |khermes {
      |  templates-path = "/tmp/khermes/templates"
      |  topic = "chustas"
      |  template-name = "chustasTemplate"
      |  i18n = "ES"
      |}
    """.stripMargin

  def khermesConfigContentLimited(events:Int, seconds:Int) =
    s"""
      |khermes {
      |  templates-path = "/tmp/khermes/templates"
      |  topic = "chustas"
      |  template-name = "chustasTemplate"
      |  i18n = "ES"
      |  timeout-rules {
      |    number-of-events: $events
      |    duration: $seconds seconds
      |  }
      |}
    """.stripMargin

  val templateContent =
    """
      |@import com.stratio.khermes.helpers.faker.Faker
      |
      |@(faker: Faker)
      |{
      |  "name" : "@(faker.Name.firstName)"
      |}
    """.stripMargin

  val avroContent =
    """
      |{
      |  "type": "record",
      |  "name": "myrecord",
      |  "fields":
      |    [
      |      {"name": "name", "type":"int"}
      |    ]
      |}
    """.stripMargin

  type testResult = (Int,LocalTime)

  import scala.concurrent.duration._

  "A flow creator" should {
    "be limited to the expected amount of messages per second" in {
      val events = 10000
      val seconds = 1
      val testTimeInSeconds = 10


      class FlowCreatorTester extends FlowCreator[Future[Seq[testResult]]] {
        override private[flow] def createSink(hc: AppConfig) = {
          val events = hc.timeoutNumberOfEventsOption.get
          Flow[String].scan(0)((counter, _) => counter +1).filter(n => n % events == 0).map(n => (n,LocalTime.now())).toMat(Sink.seq)(Keep.right)
        }
      }

      val hc = AppConfig(khermesConfigContentLimited(events, seconds), kafkaConfigContent, templateContent)
      val testFlow = new FlowCreatorTester().create(hc)
      val (killSwitch,probe) = testFlow.run()

      Thread.sleep(testTimeInSeconds * 1000)
      killSwitch.shutdown()
      val results = Await.result(probe, 1.seconds).map(_._2).drop(2)

      results.foreach(println)

      val millisecondsError = 100

      val nanoSecondsInSecond = Math.pow(10, 9)

      results.zip(results.tail).map(r => (nanoSecondsInSecond - (r._2.toNanoOfDay - r._1.toNanoOfDay)) / 1000000).foreach(errorTime => {
        errorTime > millisecondsError  shouldNot be(true)
        errorTime < -millisecondsError  shouldNot  be(true)
      })

      //results.zip(results.tail).map{ case (pre, post) => post.toNanoOfDay - pre.toNanoOfDay}.foreach(println)

    }
  }
}
