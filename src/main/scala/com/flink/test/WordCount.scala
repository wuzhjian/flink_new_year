package com.flink.test

import org.apache.flink.api.scala._


object WordCount {

  def main(args: Array[String]): Unit = {

    // 创建一个批处理执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中读取数据
    val inputPah = "E:\\idea\\flink_new_year\\src\\main\\resources\\hello.txt"
    val inputDateSet = env.readTextFile(inputPah)

    val wordCountDataSet = inputDateSet
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(_._1)
      .sum(1)

    // 打印输出
    wordCountDataSet.print()

  }
}
