package com.atguigu.gmall.realtime.app

import java.lang

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall.realtime.bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import com.atguigu.gmall.realtime.util.{MyKafkaUtils, MyOffsetUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
日志数据消费分流
 把kafka内ods层的原始数据读取出来，再分流进kafka构建dwd层数据
 */
object OdsBaseLogApp {

  def main(args: Array[String]): Unit = {
    //1.准备实时环境，创建streamingcontext(ssc)
    val sparkConf: SparkConf = new SparkConf().setAppName("ods_base_log_app").setMaster("local[2]")//并行度最好与消费对应kafka主题分区数保持一致
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //从kafka消费数据，拿到sparkstreaming的InputDStream流对象
    val topic : String = "ODS_BASE_LOG_0106"
    val groupId : String = "ODS_BASE_LOG_GROUP"
    //TODO 优化：从redis读取offset，指定offset进行消费
    val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffsetFromRedis(topic,groupId)

    //TODO 优化：逻辑判断，group组是否第一次消费topic数据，如果第一次消费则默认重头开始消费
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.size > 0){
      //指定offset消费
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc,topic,groupId,offsets)
    }else{
      //从头开始消费
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc,topic,groupId)
    }

    //TODO 优化：提取offset，因为不需要进行任何转化，只是提取一个数据，所以可以用transform算子
    var offsetRanges: Array[OffsetRange] = null
    val offsetDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    //3.转化结构，将consumerRecord对象中的value提取出来，转换成方便操作的格式
    //通用结构：jsonObject Map
    //专用结构：bean
    //这个地方为了适普通用，选择转换成JsonObject
    //TODO 优化：在上面通过对kafkaDStream进行了转化，拿去了里面的offset信息，所以这里使用转化后的offsetDStream流，效果是一样的
    val jsonObjDStream = offsetDStream.map(
      consumerRecord => {
        val messageValue: String = consumerRecord.value()
        //转换Json对象操作
        val jsonObject: JSONObject = JSON.parseObject(messageValue)
        jsonObject
      }
    )
    //测试消费通道是否打通
    //jsonObjDStream.print(100)


    /*
    4.分流
    分流原则：DWD层一共分为5个数据主题
    1.错误数据topic：是指只要数据内包含err的都定义为错误数据，错误数据不参与分流，直接写入错误主题中
    2.页面访问数据：
      2.1 page数据分流到页面访问topic
      2.2 display数据分流到曝光topic
      2.3 action数据分流到动作topic
    3.启动数据topic
     */
    val error_topic : String = "ERROR_TOPIC_0106"
    val page_topic : String = "PAGE_TOPIC_0106"
    val display_topic : String = "DISPLAY_TOPIC_0106"
    val action_topic : String = "ACTION_TOPIC_0106"
    val start_topic : String = "START_TOPIC_0106"

    jsonObjDStream.foreachRDD(
      //获得每段batch生成的rdd
      rdd => {
        //对rdd的每个分区进行遍历
        rdd.foreachPartition(
          //在分区遍历内部写的逻辑，就会封装进task，传给每个executor执行
          jsonObjIter => {
            //对每个分区的迭代器进行遍历，可以得到每条数据信息
            for (jsonObject <- jsonObjIter){
              //对单条数据信息进行处理
              val errObject: JSONObject = jsonObject.getJSONObject("err")
              //进行逻辑判断：是否返回了err数据
              if (errObject != null){
                //如果有err数据，直接发往err_topic
                MyKafkaUtils.sendToKafka(error_topic,jsonObject.toString())
              }else {
                //提取公共字段common
                val commonObject: JSONObject = jsonObject.getJSONObject("common")
                val ar: String = commonObject.getString("ar")
                val ba: String = commonObject.getString("ba")
                val ch: String = commonObject.getString("ch")
                val isNew: String = commonObject.getString("is_new")
                val md: String = commonObject.getString("md")
                val mid: String = commonObject.getString("mid")
                val os: String = commonObject.getString("os")
                val uid: String = commonObject.getString("uid")
                val vc: String = commonObject.getString("vc")

                //提取公共字段ts
                val ts: lang.Long = jsonObject.getLong("ts")

                //页面访问数据拆分到topic
                val pageJsonObj: JSONObject = jsonObject.getJSONObject("page")
                //逻辑判断，是否有页面数据
                if (pageJsonObj != null){
                  //提取页面数据的字段
                  val duringTime: lang.Long = pageJsonObj.getLong("during_time")
                  val pageItem: String = pageJsonObj.getString("item")
                  val pageItemType: String = pageJsonObj.getString("item_type")
                  val lastPageId: String = pageJsonObj.getString("last_page_id")
                  val pageId: String = pageJsonObj.getString("page_id")
                  val sourceType: String = pageJsonObj.getString("source_type")

                  //将字段封装到pageLog中.pageLog是自定义的bean，这里用到专用结构
                  val pageLog: PageLog = PageLog(mid,uid,ar,ch,isNew,md,os,vc,ba,pageId,lastPageId,pageItem,pageItemType,duringTime,sourceType,ts)
                  //将pageLog转换成Json字符串对象，为之后传输到kafka做准备
                  val pageLogJson: String = JSON.toJSONString(pageLog, new SerializeConfig(true)) //new SerializeConfig(true)用来告知程序在转化过程中不要调用pageLog额度get/set方法，因为pageLog是scala类，没有get/set方法
                  //将页面访问数据发送到topic
                  MyKafkaUtils.sendToKafka(page_topic,pageLogJson)

                  //曝光数据拆分到topic
                  val displayJsonArr: JSONArray = jsonObject.getJSONArray("displays")
                  if (displayJsonArr != null && displayJsonArr.size() > 0){
                    for (i <- 0 until displayJsonArr.size()){
                      val displayJsonObj: JSONObject = displayJsonArr.getJSONObject(i)
                      val displayType: String = displayJsonObj.getString("display_type")
                      val displayItem: String = displayJsonObj.getString("item")
                      val displayItemType: String = displayJsonObj.getString("item_type")
                      val displayOrder: String = displayJsonObj.getString("order")
                      val displayPosId: String = displayJsonObj.getString("pos_id")

                      val pageDisplayLog: PageDisplayLog = PageDisplayLog(mid,uid,ar,ch,isNew,md,os,vc,ba,pageId,lastPageId,pageItem,pageItemType,duringTime,sourceType,displayType,displayItem,displayItemType,displayOrder,displayPosId,ts)

                      val pageDisplayJson: String = JSON.toJSONString(pageDisplayLog,new SerializeConfig(true))

                      MyKafkaUtils.sendToKafka(display_topic,pageDisplayJson)

                    }
                  }

                  //行动数据拆分到topic
                  val actionJsonArr: JSONArray = jsonObject.getJSONArray("actions")
                  if (actionJsonArr != null && actionJsonArr.size() > 0){
                    for ( i <- 0 until actionJsonArr.size()){
                      val actionJsonObj: JSONObject = actionJsonArr.getJSONObject(i)
                      val actionId: String = actionJsonObj.getString("action_id")
                      val actionItem: String = actionJsonObj.getString("item")
                      val actionItemType: String = actionJsonObj.getString("item_type")
                      val actionTs: lang.Long = actionJsonObj.getLong("ts")

                      val pageActionLog: PageActionLog = PageActionLog(mid,uid,ar,ch,isNew,md,os,vc,ba,pageId,lastPageId,pageItem,pageItemType,duringTime,sourceType,actionId,actionItem,actionItemType,actionTs,ts)

                      MyKafkaUtils.sendToKafka(action_topic,JSON.toJSONString(pageActionLog,new SerializeConfig(true)))
                    }
                  }
                }


                //启动数据拆分到topic
                val startJsonObj: JSONObject = jsonObject.getJSONObject("start")
                if (startJsonObj != null){
                  //如果有启动数据，那么就分离出来发完start_topic
                  val entry: String = startJsonObj.getString("entry")
                  val loadingTime: lang.Long = startJsonObj.getLong("loading_time")
                  val openAdId: String = startJsonObj.getString("open_ad_id")
                  val openAdMs: lang.Long = startJsonObj.getLong("open_ad_ms")
                  val openAdSkipMs: lang.Long = startJsonObj.getLong("open_ad_skip_ms")
                  //把启动字段和公共字段封装成Bean对象
                  val startLog: StartLog = StartLog(mid,uid,ar,ch,isNew,md,os,vc,ba,entry,openAdId,loadingTime,openAdMs,openAdSkipMs,ts)
                  //把bean对象转化成json对象
                  val startLogJson: String = JSON.toJSONString(startLog,new SerializeConfig(true))
                  //把启动数据发往kafka对应的主题
                  MyKafkaUtils.sendToKafka(start_topic,startLogJson)
                }

              }
              //每条jsonObject都得过一遍的逻辑，executor端执行
            }
          //每个rdd的每个partition都要过一遍的逻辑，executor端执行
            MyKafkaUtils.flush()
          }
        )
        /*
        rdd.foreach(
          //获得rdd中的每条数据
          jsonObject => {
          }
        )
        */
        //每个rdd都得过一遍的位置，drive端执行
        MyOffsetUtils.saveOffsetToRedis(topic,groupId,offsetRanges)
      }
    )
    //foreachRDD外边，每次程序都得过一遍的位置,drive端执行

    ssc.start()
    ssc.awaitTermination()
  }
}
