package com.atguigu.redis;

import redis.clients.jedis.Jedis;

class Jedis_demon {
    public static void main(String[] args) {
        Jedis jet = new Jedis("hadoop102", 6379);
        String ping = jet.ping();
        System.out.println(ping);
    }
}