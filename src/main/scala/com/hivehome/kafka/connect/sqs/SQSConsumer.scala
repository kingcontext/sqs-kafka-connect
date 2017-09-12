package com.hivehome.kafka.connect.sqs

import javax.jms.{JMSException, MessageConsumer, Session}

import com.amazon.sqs.javamessaging.{SQSConnectionFactory, SQSSession}
import com.amazonaws.auth.{AWSCredentialsProviderChain, BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.{Region, Regions}

object SQSConsumer {

  def apply(conf: Conf): MessageConsumer = {
    val chain = buildCredentialsProviderChain(conf)
    createSQSConsumer(conf, chain)
  }

  @throws(classOf[JMSException])
  private def createSQSConsumer(conf: Conf, chain: AWSCredentialsProviderChain): MessageConsumer = {
    val region = Regions.fromName(conf.awsRegion)
    val connectionFactory = SQSConnectionFactory.builder
      .withRegion(Region.getRegion(region))
      .withAWSCredentialsProvider(chain)
      .build

    val connection = connectionFactory.createConnection
    val session = connection.createSession(false, SQSSession.UNORDERED_ACKNOWLEDGE )
    // In client acknowledge mode, acknowledging a message also implies acknowledging all previous messages
    // This leads to log warnings since we attempt to ackownledge all messages individually
    // val session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)
    val queue = session.createQueue(conf.queueName.get)
    val consumer = session.createConsumer(queue)
    connection.start()
    consumer
  }

  private def buildCredentialsProviderChain(conf: Conf): AWSCredentialsProviderChain = {
    (conf.awsKey, conf.awsSecret, conf.awsProfile) match {
      case (Some(key), Some(secret), _) =>
        val credentials = new BasicAWSCredentials(key, secret)
        new AWSCredentialsProviderChain(new AWSStaticCredentialsProvider(credentials), new DefaultAWSCredentialsProviderChain)
      case (_, _, Some(profile)) =>
        new AWSCredentialsProviderChain(new ProfileCredentialsProvider(profile), new DefaultAWSCredentialsProviderChain)
      case _ =>
        new DefaultAWSCredentialsProviderChain
    }
  }
}
