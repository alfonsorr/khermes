package com.stratio.khermes.flow

import java.util.concurrent.TimeUnit

import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import akka.stream.{KillSwitches, ThrottleMode, UniqueKillSwitch}
import akka.{Done, NotUsed}
import com.stratio.khermes.commons.config.AppConfig
import com.stratio.khermes.helpers.faker.Faker
import com.stratio.khermes.helpers.twirl.TwirlHelper
import com.stratio.khermes.metrics.KhermesMetrics
import com.typesafe.config.{Config, ConfigObject, ConfigValue}
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData.Record
import org.apache.kafka.clients.producer.ProducerRecord
import play.twirl.api.Txt
import tech.allegro.schema.json2avro.converter.JsonAvroConverter

import scala.collection.immutable.Iterable
import scala.concurrent.Future
import scala.concurrent.duration._

object FlowCreator extends FlowCreator[Future[Done]] {

  private val TIMEOUT_CLOSE = 60.seconds
  private val parallelism = 100
  private val dispatcher = "akka.kafka.default-dispatcher"

  override private[flow] def createSink(hc: AppConfig) = {

    def parseKafkaClientsProperties(config: Config): Map[String, String] = {
      def collectKeys(c: ConfigObject, prefix: String, keys: Set[String]): Set[String] = {
        var result = keys
        val iter = c.entrySet.iterator
        while (iter.hasNext) {
          val entry = iter.next()
          entry.getValue match {
            case o: ConfigObject =>
              result ++= collectKeys(o, prefix + entry.getKey + ".", Set.empty)
            case s: ConfigValue =>
              result += prefix + entry.getKey
            case _ =>
            // in case there would be something else
          }
        }
        result
      }

      val keys = collectKeys(config.root, "", Set.empty)
      keys.map(key => key -> config.getString(key)).toMap
    }

    val kafkaConf = parseKafkaClientsProperties(hc.kafkaConfig.getConfig("kafka"))
    val producerSettings = new ProducerSettings(kafkaConf, None, None,TIMEOUT_CLOSE ,parallelism,dispatcher)
    hc.avroSchema.map(new Parser().parse(_))
      .map(
        schema => {
          val converter = new JsonAvroConverter()
          val avroSink = Producer.plainSink(producerSettings.asInstanceOf[ProducerSettings[String, Record]])
          Flow[String].map(s => {
            val dataRecord = converter.convertToGenericDataRecord(s.getBytes("UTF-8"), schema)
            new ProducerRecord[String, Record](hc.topic, dataRecord)
          }).toMat(avroSink)(Keep.right)
        }
      ).getOrElse(
      Flow[String].map(s => new ProducerRecord[String, String](hc.topic, s))
        .toMat(Producer.plainSink(producerSettings.asInstanceOf[ProducerSettings[String, String]]))(Keep.right)
    )
  }
}

trait FlowCreator[M] extends KhermesMetrics {

  /**
    * Flow ready to execute that can be stopped thanks to the killSwitch object materialized
    * @param hc     with the Hermes' configuration.
    * @param config with general configuration.
    */
  def create(hc: AppConfig)(implicit config: Config): RunnableGraph[(UniqueKillSwitch,M)] = {


    val producer = createSource(hc)
    val sink = createSink(hc)

    producer.via(createSpeedLimiterIfConfigured(hc))
      .via(createMetricFlow())
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(sink)(Keep.both)
  }

  private def createSource(hc: AppConfig)(implicit config: Config): Source[String, NotUsed] = {
    val template = TwirlHelper.template[(Faker) => Txt](hc.templateContent, hc.templateName)
    val khermes = Faker(hc.khermesI18n)

    val templateIterator: Iterable[String] = new Iterable[String]{
      override def iterator: Iterator[String] = new Iterator[String]{
        override def hasNext: Boolean = true

        override def next(): String = template.static(khermes).toString
      }
    }
    Source[String](templateIterator)
  }

  private[flow] def createSink(hc: AppConfig): Sink[String, M]

  private def createMetricFlow[In]():Flow[In, In, NotUsed] = {

    val messageSentCounterMetric = getAvailableCounterMetrics("khermes-messages-count")
    val messageSentMeterMetric = getAvailableMeterMetrics("khermes-messages-meter")

    Flow[In].map(e => {
      messageSentCounterMetric.foreach(_.inc())
      messageSentMeterMetric.foreach(_.mark())
      e
    })
  }

  private def createSpeedLimiterIfConfigured[In](hc: AppConfig):Flow[In,In,NotUsed] = {
    (for {
      duration <- hc.timeoutNumberOfEventsDurationOption
      nEvents <- hc.timeoutNumberOfEventsOption
    } yield Flow[In].throttle(nEvents, FiniteDuration(duration.getSeconds, TimeUnit.SECONDS), nEvents, ThrottleMode.Shaping))
      .getOrElse(Flow[In])
  }
}
