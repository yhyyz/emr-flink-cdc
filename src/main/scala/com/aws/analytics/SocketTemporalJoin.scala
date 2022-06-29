package com.aws.analytics

import com.aws.analytics.conf.Config
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{FieldExpression, UnresolvedFieldExpression}
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, dataStreamConversions}
import org.apache.flink.api.scala._

import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.collection.JavaConverters._
import org.apache.flink.table.functions.TemporalTableFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.expressions.PlannerExpressionParserImpl.proctime



object SocketTemporalJoin {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(30* 1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    val rocksBackend: StateBackend = new RocksDBStateBackend("s3://app-util/chp/")
    env.setStateBackend(rocksBackend)

    val tEnv = StreamTableEnvironment.create(env)
    import org.apache.flink.api.scala._

    val dataTB = env.socketTextStream("localhost",9000).map(line=>
      Tuple2(line.split(",")(0),line.split(",")(1)))
      .toTable(tEnv,$"id", $"name",'proctime.proctime)
    val dimTB = env.socketTextStream("localhost",9001).map(line=>
      Tuple2(line.split(",")(0),line.split(",")(1))).toTable(tEnv,$"id", $"name",'proctime.proctime)

    val dim_fun = dimTB.createTemporalTableFunction($"proctime", $"id")
    tEnv.registerFunction("dim_fun",dim_fun)
    tEnv.createTemporaryView("source", dataTB)
    tEnv.createTemporaryView("dim", dimTB)

    val sql = tEnv.executeSql("select t1.*,t2.* from  source as t1, " +
      " Lateral table (dim_fun(t1.proctime)) as t2 where t1.id=t2.id")

    sql.print()
    tEnv.execute("join demo")
  }

}
