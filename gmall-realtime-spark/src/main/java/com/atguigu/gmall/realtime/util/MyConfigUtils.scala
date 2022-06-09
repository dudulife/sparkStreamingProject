package com.atguigu.gmall.realtime.util
/*
配置类
可以后期改动，保存kafka集群在配置文件中的key信息
 */
object MyConfigUtils {
  //kafka.servers
  val KAFKA_SERVERS : String = "kafka.broker.list"
  //redis.hosts
  val REDIS_HOST = "redis.host"
  //redis.port
  val REDIS_PORT = "redis.port"
}
