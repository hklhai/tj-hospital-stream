package com.hxqh.batch

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/**
  * Created by Ocean lin on 2020/1/4.
  *
  * @author Ocean lin
  */
object WordCountScala {
  def main(args: Array[String]): Unit = {
    val parameter = ParameterTool.fromArgs(args)
    val environment = ExecutionEnvironment.getExecutionEnvironment

    val dataSet =
      if (parameter.has("input")) {
        environment.readTextFile(parameter.get("input"))
      } else {
        environment.fromCollection(WordCountData.WORDS);
      }

    val count = dataSet.flatMap(_.toLowerCase().split(" ")).filter(_.nonEmpty)
      .map((_, 1)).groupBy(0).sum(1);

    if (parameter.has("output")) {
      count.writeAsCsv(parameter.get("output"), "\n", " ")
      count.print()
    } else {
      count.print()
    }


  }
}
