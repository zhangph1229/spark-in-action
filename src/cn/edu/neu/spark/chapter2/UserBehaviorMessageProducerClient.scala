package cn.edu.neu.spark.chapter2.test

import java.util.Properties
import java.util.Random
import kafka.producer.ProducerConfig
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.KeyedMessage

object UserBehaviorMessageProducerClient {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      //输入kafka参数
      println("Usage: UserBehaviorMessageProducer ubuntu1:9092 user-behavior-topic")
      System.exit(1)
    }
    new Thread(new UserBehaviorMessageProducer(args(0), args(1))).start() //开启线程
  }
}

class UserBehaviorMessageProducer(brokers: String, topic: String) extends Runnable {

  private val brokerList = brokers
  //kafka的broker地址
  private val targetTopic = topic //目标消息主题名称

  private val props = new Properties() //生成properties文件
  props.put("metadata.broker.list", this.brokerList) //metadata.broker.list
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("producer.type", "async")
  private val config = new ProducerConfig(this.props)
  //加载配置
  private val producer = new Producer[String, String](this.config)

  private val PAGE_NUM = 100
  //网页的数量
  private val MAX_MSG_NUM = 3
  //网页信息的最大数量
  private val MAX_CLICK_TIME = 5
  //网站的最大浏览次数 以及 浏览时间的设置
  private val MAX_STAY_TIME = 10
  //最大的停留时间
  private val LIKE_OR_NOT = Array[Int](1, 0, -1) //是否点赞

  def run(): Unit = {
    val rand = new Random() //  产生随机数
    while (true) {
      //产生用户行为信息
      val msgNum = rand.nextInt(MAX_MSG_NUM) + 1 //每次产生的数据量
      //按照格式 （pageID|浏览次数|停留时间|是否点赞）生成用户行为信息
      try {
        for (i <- 0 to msgNum) {
          var msg = new StringBuilder() //StringBuilder用来构建字符串
          /**
            * 构造字符串的三个步骤
            * 1、StringBuilder sb = new StringBuilder()
            * 2、sb.append(str)
            * 3、sb.toString()
            */
          msg.append("page" + rand.nextInt(PAGE_NUM) + 1) //pageID
          msg.append("|")
          msg.append(rand.nextInt(MAX_CLICK_TIME) + 1)
          msg.append("|")
          msg.append(rand.nextInt(MAX_STAY_TIME) + rand.nextFloat())
          msg.append("|")
          msg.append(LIKE_OR_NOT(rand.nextInt(3)))
          println(msg.toString())
          sendMessage(msg.toString) //send the generated message to broker
        }
        println("%d user behavior messages produced.".format(msgNum + 1))
      } catch {
        case e: Exception => println(e)
      }
      try {
        //程序休眠10秒钟
        Thread.sleep(30000)
      } catch {
        case e: Throwable => e.printStackTrace() // TODO: handle error
      }
    }
  }

  def sendMessage(arg: String) = {
    try {
      val data = new KeyedMessage[String, String](this.topic, arg)
      producer.send(data)
    } catch {
      case e: Exception => println(e)
    }
  }
}