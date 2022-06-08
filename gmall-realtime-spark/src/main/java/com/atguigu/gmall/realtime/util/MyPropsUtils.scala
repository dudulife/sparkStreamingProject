package com.atguigu.gmall.realtime.util

import java.util.ResourceBundle

/*
配置文件解析
工具类不能把kafka集群位置写死，设计成从配置文件中读取kafka集群的位置，
这样集群位置改变后可以修改配置文件，不影响代码
*/
object MyPropsUtils {

  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  def apply(key : String) : String={
    bundle.getString(key)
  }

  def main(args: Array[String]): Unit = {
    println(MyPropsUtils.apply("kafka.broker.list"))
  }
}
