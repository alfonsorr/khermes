package com.stratio.khermes.flow

import java.util.concurrent.TimeUnit

import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{Flow, Keep, Merge, RunnableGraph, Sink, Source}
import akka.stream.{KillSwitches, ThrottleMode, UniqueKillSwitch}
import akka.{Done, NotUsed}
import com.stratio.khermes.commons.config.AppConfig
import com.stratio.khermes.helpers.faker.Faker
import com.stratio.khermes.helpers.twirl.TwirlHelper
import com.stratio.khermes.metrics.KhermesMetrics
import com.typesafe.config.Config
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData.Record
import org.apache.kafka.clients.producer.ProducerRecord
import play.twirl.api.Txt
import tech.allegro.schema.json2avro.converter.JsonAvroConverter

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object FlowCreator extends FlowCreator[Future[Done]] {
  override private[flow] def createSink(hc: AppConfig) = {
    val converter = new JsonAvroConverter()
    val producerSettings = ProducerSettings(hc.kafkaConfig, None, None)
    hc.avroSchema.map(new Parser().parse(_))
      .map(
        schema => {

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
    val a = Source.fromIterator[String](() => Iterator.continually[String](template.static(khermes).toString))
    val b = Source.fromIterator[String](() => Iterator.continually[String](template.static(khermes).toString))

    //Source.combine(a,b)(Merge(_))
    a
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
