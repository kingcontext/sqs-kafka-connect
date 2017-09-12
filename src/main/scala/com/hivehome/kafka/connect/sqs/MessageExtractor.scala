package com.hivehome.kafka.connect.sqs

import javax.jms.Message

import org.apache.avro.{Schema => AvroSchema}
import com.amazon.sqs.javamessaging.message.{SQSBytesMessage, SQSObjectMessage, SQSTextMessage}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.connect.data.{Schema, SchemaAndValue}
import io.confluent.connect.avro.AvroData
import org.slf4j.LoggerFactory

class AvroMessageExtractor(val schema: AvroSchema) extends MessageExtractor{

  val logger = LoggerFactory.getLogger(getClass.getName)

  private val reader : GenericDatumReader[GenericRecord] = new GenericDatumReader(schema)
  private val avroData : AvroData = new AvroData(10);

  override def value(s: String): SchemaAndValue = {
    logger.trace(s);

    /*
    var ss = s.replace("campaign name", "campaign_name");
    if (!ss.contains("\"reason\"")) {
      ss = ss.substring(0, ss.length -1) + ", \"reason\": \"\"}";
    }
    if (ss.contains("\"URL\"")) {
      ss = ss.replace("\"URL\"", "\"url\"");
    }
    else if (!ss.contains("\"url\"")) {
      ss = ss.substring(0, ss.length -1) + ", \"url\": \"\"}";
    }
    if (!ss.contains("\"sending_ip\"")) {
      ss = ss.substring(0, ss.length -1) + ", \"sending_ip\": \"\"}";
    }
    if (!ss.contains("\"tag\"")) {
      ss = ss.substring(0, ss.length -1) + ", \"tag\": \"\"}";
    }
    ss = ss.replaceAll("[\\t\\n\\r]+"," ") //ss.replaceAll("(\r\n)|\r|\n", "")
    */
    val avroRecord : GenericRecord = reader.read(null, DecoderFactory.get().jsonDecoder(schema, s.replaceAll("[\\t\\n\\r]+"," ")));
    avroData.toConnectData(schema, avroRecord);
  }
}

class MessageExtractor {

  private val defaultValueSchema = org.apache.kafka.connect.data.Schema.STRING_SCHEMA

  def extract(msg: Message): SchemaAndValue = value(toString(msg))

  def toString(msg: Message): String = msg match {
    case text: SQSTextMessage => text.getText
    case bytes: SQSBytesMessage => new String(bytes.getBodyAsBytes)
    case objectMsg: SQSObjectMessage => objectMsg.getObject.toString
    case _ => msg.toString
  }

  def value(s: String): SchemaAndValue = {
    new SchemaAndValue(defaultValueSchema, s)
  }
}

object MessageExtractor {

  def apply(msg: Message): String = new MessageExtractor().extract(msg).value().toString
}