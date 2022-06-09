package com.atguigu.gmall.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * redis工具类，主要是用于获取jedis对象
 * jedis：实现redis的java代码操作
 * 为啥要写这个工具类，因为需要把offset管理到redis里面
 */
object MyRedisUtils {

  //创建jedispool
  private var jedisPool : JedisPool= null

  private def getJedisPool(): JedisPool ={

    if (jedisPool == null){

      val host: String = MyPropsUtils.apply(MyConfigUtils.REDIS_HOST)

      val port: Int = MyPropsUtils.apply(MyConfigUtils.REDIS_PORT).toInt

      val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(10)//设置最大连接数
      jedisPoolConfig.setMaxIdle(5)//设置最大闲时连接数
      jedisPoolConfig.setMinIdle(5)//设置最小闲时连接数
      jedisPoolConfig.setBlockWhenExhausted(true)//设置忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(2000)//设置最大等待时长
      jedisPoolConfig.setTestOnBorrow(true)//设置每次连接时是否进行测试

      jedisPool = new JedisPool(jedisPoolConfig,host,port)
    }
    jedisPool
  }

  //从jedispool中获取jedis对象
  def getJedisFromPool() : Jedis = {

    getJedisPool().getResource

  }

  //把jedis还回jedispool
  def close(jedis :Jedis) : Unit = {
    jedis.close()
  }

  //测试jedis是否ping通
  def main(args: Array[String]): Unit = {
    val jedis: Jedis = getJedisFromPool()
    println(jedis.ping())
    close(jedis)
  }

}
