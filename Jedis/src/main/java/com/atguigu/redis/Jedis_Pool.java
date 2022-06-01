package com.atguigu.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.ZAddParams;

import java.util.List;

public class Jedis_Pool {

    public static JedisPool jedispool=null;

    /*从jedis连接池中获得jedis对象的方法*/
    public static Jedis getJedisFromPool(){
        if (jedispool==null){
            String host = "hadoop102";
            int ip = 6379;

            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(10);
            jedisPoolConfig.setMaxIdle(5);
            jedisPoolConfig.setMinIdle(5);
            jedisPoolConfig.setBlockWhenExhausted(true);
            jedisPoolConfig.setMaxWaitMillis(2000);
            jedisPoolConfig.setTestOnBorrow(true);

            jedispool= new JedisPool(jedisPoolConfig, host, ip);
        }
        Jedis jedis = jedispool.getResource();
        return jedis;
    }

    /*原始获得jedis对象的方法*/
    public static Jedis getJedis(){
        String host = "hadoop102";
        int port = 6379 ;
        Jedis jedis = new Jedis(host, port );
        return jedis ;
    }

    public static void main(String[] args) {
/*        Jedis jedis = getJedisFromPool();
        //测试jedis对象是否获得成功
        System.out.println(jedis.ping());

        jedis.set("user:name","shanggiugu");
        String name = jedis.get("user:name");
        System.out.println(name);

        jedis.close();*/

        testString();

        testList();

        testSet();
    }
    /**
     * 测试String类型的方法
     */
    public static void testString(){
        Jedis jedis = getJedisFromPool();
        jedis.mset("name","panpan","gender","women","addr","beijing");
        List<String> mget = jedis.mget("name", "gender", "addr");
        System.out.println(mget);
        jedis.close();
    }
    /**
     * 测试List类型的方法
     */
    public static void testList(){
        Jedis jedis = getJedisFromPool();
        jedis.lpush("l5","v1");
        System.out.println(jedis.llen("l5"));
        System.out.println(jedis.lrange("l5", 0, -1));
        jedis.close();
    }

    /**
     * 测试Set类型的方法
     */
    public static void testSet(){
        Jedis jedis = getJedisFromPool();
        jedis.sadd("ss","a","b","c");
        System.out.println(jedis.smembers("ss"));
        jedis.close();
    }

    /**
     * 测试Zset类型的方法
     */
    public static void testZset(){
        Jedis jedis = getJedisFromPool();
        jedis.zadd("zz",100,"scala",new ZAddParams());

        jedis.close();
    }

    /**
     * 测试Hash类型的方法
     */
    public static void testHash(){

    }

}
