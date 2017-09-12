/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  * <p>
  * http://www.apache.org/licenses/LICENSE-2.0
  * <p>
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  **/
package com.hivehome.kafka.connect.sqs

import java.util.{List => JList, Map => JMap}
import javax.jms._

import org.apache.avro.{Schema => AvroSchema}
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

object SQSSourceTask {
  private val SqsQueueField: String = "queue"
  private val MessageId: String = "messageId"
  private val KeySchema = Schema.STRING_SCHEMA
}

class SQSSourceTask extends SourceTask {
  val logger = LoggerFactory.getLogger(getClass.getName)
  private var conf: Conf = _
  private var consumer: MessageConsumer = null
  // MessageId to MessageHandle used to ack the message on the commitRecord method invocation
  private var unAcknowledgedMessages = Map[String, Message]()
  private var valueSchema : AvroSchema = null
  private var messageExtractor : MessageExtractor = null

  def version: String = Version()

  def start(props: JMap[String, String]): Unit = {
    conf = Conf.parse(props.asScala.toMap).get
    start(conf)
  }

  def start(conf: Conf): Unit = {
    val schemaString : Option[String] = conf.valueSchema
    if (schemaString.isDefined) {
      val parser: AvroSchema.Parser = new AvroSchema.Parser()
      valueSchema = parser.parse(conf.valueSchema.get)
      messageExtractor = new AvroMessageExtractor(valueSchema)
    }
    else {
      messageExtractor = new MessageExtractor
    }

    logger.debug("Creating consumer...")
    synchronized {
      try {
        consumer = SQSConsumer(conf)
        logger.info("Created consumer to SQS topic {} for reading", conf.queueName)
      }
      catch {
        case NonFatal(e) => logger.error("Exception", e)
      }
    }
  }

  import com.hivehome.kafka.connect.sqs.SQSSourceTask._

  @throws(classOf[InterruptedException])
  def poll: JList[SourceRecord] = {
    def toRecord(msg: Message): SourceRecord = {
      val extracted = messageExtractor.extract(msg)
      val key = Map(SqsQueueField -> conf.queueName.get).asJava
      val value = Map(MessageId -> msg.getJMSMessageID).asJava
      new SourceRecord(key, value, conf.topicName.get, KeySchema, msg.getJMSMessageID, extracted.schema(), extracted.value())
    }

    assert(consumer != null) // should be initialised as part of start()
    Try {
      Option(consumer.receive).map { msg =>
        logger.debug("Received message {}", msg.getJMSMessageID)

        // This operation is not threadsafe as a result the plugin is not threadsafe.
        // However KafkaConnect assigns a single thread to each task and the poll
        // method is always called by a single thread.
        unAcknowledgedMessages = unAcknowledgedMessages.updated(msg.getJMSMessageID, msg)

        toRecord(msg)
      }.toSeq
    }.recover {
      case NonFatal(e) =>
        logger.error("Exception while processing message", e)
        List.empty
    }.get.asJava
  }

  @throws(classOf[InterruptedException])
  override def commitRecord(record: SourceRecord): Unit = {
    val msgId = record.sourceOffset().get(MessageId).asInstanceOf[String]
    val maybeMsg = unAcknowledgedMessages.get(msgId)
    maybeMsg.foreach(_.acknowledge())
    unAcknowledgedMessages = unAcknowledgedMessages - msgId
  }

  def stop() {
    logger.debug("Stopping task")
    synchronized {
      unAcknowledgedMessages = Map()
      try {
        if (consumer != null) {
          consumer.close()
          logger.debug("Closed input stream")
        }
      }
      catch {
        case NonFatal(e) => logger.error("Failed to close consumer stream: ", e)
      }
      this.notify()
    }
  }
}