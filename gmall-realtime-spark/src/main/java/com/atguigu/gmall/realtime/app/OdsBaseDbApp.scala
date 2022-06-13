package com.atguigu.gmall.realtime.app

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.util.{MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * 业务数据消费分流
 * 1.准备实时环境
 * 2.从redis获取offset
 * 3.从kafka消费数据
 * 4.从kafka流中提取offset
 * 5.kafka流数据结构转化
 * 6.数据处理
 * 7.刷写kafka生产者缓冲区
 * 8.提交offset到redis
 */
object OdsBaseDbApp {

  def main(args: Array[String]): Unit = {
    //1.准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("ods_base_db_app").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    //2.从redis中获取offset
    val topic: String = "ODS_BASE_DB_0106"
    val groupId: String = "ODS_BASE_DB_GROUP"
    val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffsetFromRedis(topic,groupId)

    //3.从kafka消费数据
    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.size > 0){
      inputDStream = MyKafkaUtils.getKafkaDStream(ssc,topic,groupId,offsets)
    }else {
      inputDStream = MyKafkaUtils.getKafkaDStream(ssc,topic,groupId)
    }

    //4.从kafka流中提取offsets
    var offsetRanges: Array[OffsetRange] = null
    val offsetsDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    //5.转换流结构
    val jsonObjDStream: DStream[JSONObject] = offsetsDStream.map(
      comsumerRecord => {
        val messageValue: String = comsumerRecord.value()
        val jsonObj: JSONObject = JSON.parseObject(messageValue)
        jsonObj
      }
    )
    //测试通道
    //jsonObjDStream.print(100)

    //6.数据处理：分流
    //分流原则：
    //  事实表数据：以表为基础，以操作类型为单位，将一个表的数据分流到多个topic
    //  维度表数据：分流到redis

    //创建事实表、维度表的表清单，以便在分流的时候根据数据的表名进行判断分流
//    val factTable: Set[String] = Set[String]("order_info","order_detail")
//    val dimTable: Set[String] = Set[String]("user_info","base_province")

    jsonObjDStream.foreachRDD(
    //获得流中每个batch的rdd批次数据
    rdd => {

      //TODO 如何动态维护表清单？
      //TODO 将表清单维护到redis中，代码循环读取redis内对应的表清单，如果要修改表清单，那么在Redis中修改即可
      //TODO 考虑将代码写在foreachRDD内部顶头位置，这样在执行每个batch的RDD处理逻辑之前，就会先获取一次表清单的内容
      //1.类型：set
      //2.key：FACT:TABLES   DIM:TABLES
      //3.value:[表名1，表名2...]
      //4.存储方法：sadd
      //5.读取方法：smembers
      //6.是否过期：不过期
      val jedis: Jedis = MyRedisUtils.getJedisFromPool()
      val dwdKey : String = "FACT:TABLES"
      val dimKey : String = "DIM:TABLES"
      val factTables: util.Set[String] = jedis.smembers(dwdKey)
      println("factTables:" + factTables)
      val dimTables: util.Set[String] = jedis.smembers(dimKey)
      println("dimTables:" + dimTables)
      //做成广播变量，方便发送到每个executor进行读取
      val factTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(factTables)
      val dimTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dimTables)
      MyRedisUtils.close(jedis)


        rdd.foreachPartition(
          jsonObjIter => {

            //TODO 考虑将jedis维护在foreachpartition内，这样一个分区（一个executor的一个task）内部共用一个jedis对象
            val jedis: Jedis = MyRedisUtils.getJedisFromPool()

            for (jsonObj <- jsonObjIter){
              //获取每条数据的表名
              val tableName: String = jsonObj.getString("table")
              //获取每条数据插入的类型
              val operType: String = jsonObj.getString("type")
              //根据类型字段判断数据的变更类型,同时剔除错误数据
              val operResult: String = operType match {
                case "bootstrap-insert" => "I"
                case "insert" => "I"
                case "update" => "U"
                case _ => null
              }

              if (operResult != null){
                //进行事实表分流
                if (factTablesBC.value.contains(tableName)){
                  //只需要maxwell传输的数据中，“data”字段内最新的数据
                  val dataJsonObj: JSONObject = jsonObj.getJSONObject("data")
                  //topicname考虑形式：ORDER_INFO_I,ORDER_INFO_U
                  val topicName : String = s"${tableName.toUpperCase()}_$operResult"
                  //将数据写入kafka中
                  MyKafkaUtils.sendToKafka(topicName,dataJsonObj.toString())
                }

                //模拟数据延迟


                //TODO 历史维度表的数据，如果不修改的情况如何进入redis？
                //TODO 写一个脚本，让maxwell将mysql中全部数据bootstrap一次
                //TODO 然后在分流的时候识别type
                //进行维度表分流
                if (dimTablesBC.value.contains(tableName)){
                  //维度表数据考虑放在redis中，这里又需要进行redis需求的六大问题
                  /*
                  1.使用什么类型存储
                  string
                  2.key怎么设计
                  DIM:[table]:[id]
                  3.value怎么设计
                  整条数据的json格式字符串
                  4.使用什么方法存入
                  set
                  5.使用什么方法读取
                  get
                  6.是否过期
                  不过期
                  */
                  //获取redis对象
                  //TODO:在此处开辟redis连接好不好？不好，因为每一条数据都需要一个新的jedis对象
                  //TODO 考虑将jedis对象维护起来，多条数据共用一个jedis对象
//                  val jedis: Jedis = MyRedisUtils.getJedisFromPool()
                  //制作key
                  val dataJsonObj: JSONObject = jsonObj.getJSONObject("data")
                  val dataId: String = dataJsonObj.getString("id")
                  val redisKey : String = s"DIM:${tableName.toUpperCase()}:$dataId"
                  //写入redis
                  jedis.set(redisKey,dataJsonObj.toString())
//                  MyRedisUtils.close(jedis)
                }
              }
              //每条数据都需要执行，在executor端执行
            }
            //每批次rdd的每个分区都需要执行，在executor端执行
            MyKafkaUtils.flush()
            MyRedisUtils.close(jedis)
          }
        )
      //每批次rdd都需要执行，在driver端实行
        MyOffsetUtils.saveOffsetToRedis(topic,groupId,offsetRanges)
      }
    )
    ssc.start()
    ssc.awaitTermination()

  }

}
