package com.atguigu.gmall.realtime.util

import java.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/*
kafka工具类，用于消费和生产数据
 */
object MyKafkaUtils {
  /*kafka消费者配置
  1.消费者配置类：consumerconfig，配消费者config参数
  2.根据config创建消费者
   */
  private val consumerParams: mutable.Map[String,Object] = mutable.Map[String,Object](
    //kafka集群位置
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils.apply(MyConfigUtils.KAFKA_SERVERS),
    //key和value的反序列化器
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    //消费者组，这个配置是动态的，设计成参数传入
    //offset重置
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    //自动提交offset
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    //offset提交频率
    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "5000"
  )

  /*
  从kafka消费数据，获取inputDStream对象
  kafka消费数据，需要指定
  1.消费的topic
  2.指定消费组
  3.ssc：sparkstreaming非常重要的一个实时对象，只能调用方法的时候传入
   */
  def getKafkaDStream(ssc: StreamingContext,topic :String,groupId :String): InputDStream[ConsumerRecord[String,String]] = {
    //设置消费者组
    consumerParams.put(ConsumerConfig.GROUP_ID_CONFIG,groupId)

    val inputDStream :InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,//位置策略
      ConsumerStrategies.Subscribe[String,String](Array(topic),consumerParams)//消费者策略，第一个参数需要可迭代的，所以传入array（topic）
    )
    inputDStream
  }

  /*
  创建kafka生产者对象
  1.创建config对象，配参数
  2.根据config创建生产者
   */
  private def createProducer(): KafkaProducer[String , String]  = {

    /*kafka生产者配置
 生产者配置类：producerConfig
  */
    val producerParams:util.HashMap[String , AnyRef]= new util.HashMap[String , AnyRef]()
    //kafka集群位置
    producerParams.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , MyPropsUtils.apply(MyConfigUtils.KAFKA_SERVERS))
    //key，value序列化
    producerParams.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , "org.apache.kafka.common.serialization.StringSerializer")
    producerParams.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , "org.apache.kafka.common.serialization.StringSerializer")
    //还有一些参数直接采用默认值
    //acks：数据从sender发到kafka集群后，kafka返回的结果，默认是1
    //buffer/memory:main线程到sender线程中间的缓冲区，默认是32m
    //retries:数据发送失败是否重新发送
    //batch.size:数据在缓冲区达到batch.size大小就会被sender拉走
    //linger.ms:数据在缓冲区达到alinger.ms时间就会被sender拉走
    //enable.idempotence:是否开启幂等性，默认是false

    //根据config配置创建kafka生产者
    val producer: KafkaProducer[String, String] = new KafkaProducer[String ,String](producerParams)
    producer
  }

  /*
  获取kafka生产者对象
   */
  private val producer: KafkaProducer[String, String] = createProducer()

  /*
  往kafka中生产数据
  需要传入指定的topic，和需要发送的消息
   */
  def sendToKafka(topic : String,message :String) : Unit ={
    producer.send(new ProducerRecord[String,String](topic,message))
  }

}
