package com.atguigu.gmall.realtime.util

import java.util

import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis
import java.util.HashMap

import org.apache.commons.lang.mutable.Mutable
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

/**
 * kafka 偏移量管理工具类
 * 这个类主要实现把offset写入到redis，然后再从redis读取offset
 */
object MyOffsetUtils {
  /**
   * kafka中存储offset也是key-value的格式：
   * key：group.id + topic + partition分区号(俗称GTP)
   * value：offset的值
   *
   * redis怎么存储offset，回忆redis需求五大问
   * 1.用什么类型存？
   *  hash
   * 2.key怎么设计？
   *  取kafka的key内的前两个，offset:[topic]:[group]
   * 3.value怎么设计？
   *  value的key为[partition]，value的value为[offset]
   * 4.写入用什么命令？
   *  hset
   * 5.查询用什么命令？
   *  hgetall,一个消费者组如果读取offset说明它要开始消费了，所以需要得到topic里所有分区的offset，接着消费每个分区，因此用hgetall。
   */

  //把offset写入redis
  //从InputDStream[ConsumerRecord[String, String]]中获取offset信息
  //需要传入：topic，group，offset信息
  def saveOffsetToRedis(topic : String,group : String,offsetRanges : Array[OffsetRange]): Unit ={
    if ( offsetRanges != null && offsetRanges.size > 0){
      //获取jedis，通过jedis稍后把每一条offset存入redis
      val jedis: Jedis = MyRedisUtils.getJedisFromPool()
      //创建hash的map对象
      val hashMap: HashMap[String, String] = new HashMap[String,String]()
      //遍历offset数组
      for (i <- 0 until offsetRanges.size){
        val offsetRange: OffsetRange = offsetRanges(i)
        val partition: Int = offsetRange.partition
        val offset: Long = offsetRange.untilOffset
        hashMap.put(partition.toString,offset.toString)
      }

      println("存储offset：" + hashMap)
      //创建redis的key
      val redisKey: String = s"offset:$topic:$group"
      //把offset数据发往redis
      jedis.hset(redisKey,hashMap)
      MyRedisUtils.close(jedis)
    }
  }

  //把从redis读取offset
  //需要指定redis的key来读取，存储进redis的key有topic和group数据
  //所以参数需要传入topic，group
  def readOffsetFromRedis(topic : String,group : String) : Map[TopicPartition,Long] = {
    val redisKey: String = s"offset:$topic:$group"
    //获取jedis对象
    val jedis: Jedis = MyRedisUtils.getJedisFromPool()
    //从redis取出offset信息
    val hashMap: util.Map[String, String] = jedis.hgetAll(redisKey)
    //构建需要返回的offset信息，目标就是将util.Map[String, String]，转化为mutable.Map[TopicPartition, Long]
    val offsets: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition,Long]()
    import scala.collection.JavaConverters._
    //把java类型的hashmap通过asscala转化成scala的类，然后就可以使用模式匹配了
    for ((partition,offset) <- hashMap.asScala){
      val topicPartition: TopicPartition = new TopicPartition(topic,partition.toInt)
      offsets.put(topicPartition,offset.toLong)
    }
    //循环完成，那么转化也完成，返回offset就好了
    MyRedisUtils.close(jedis)
    //offset参数要求是不可变Map，转一下咯
    offsets.toMap
  }

}
