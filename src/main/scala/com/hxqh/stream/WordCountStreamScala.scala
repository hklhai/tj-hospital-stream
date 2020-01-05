package com.hxqh.stream

import com.hxqh.batch.WordCountData
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

/**
  * Created by Ocean lin on 2020/1/5.
  *
  * @author Ocean lin
  */
object WordCountStreamScala {


  def main(args: Array[String]): Unit = {

    val parameter = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream = if (parameter.has("input")) {
      env.readTextFile(parameter.get("input"))
    } else {
      // 转换为可变参数类型
      env.fromElements(WordCountData.WORDS: _*)
    }

    val sum = dataStream.flatMap(_.toLowerCase().split(" "))
      .filter(_.nonEmpty).map((_, 1)).keyBy(0).sum(1)

    if (parameter.has("output")) {
      sum.writeAsText(parameter.get("output"))
    } else {
      sum.print()
    }
    env.execute("Word Count !")

  }
}
