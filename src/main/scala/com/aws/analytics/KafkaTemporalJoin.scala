package com.aws.analytics

import com.aws.analytics.Kafka2Hudi.gson
import com.aws.analytics.MySQLCDC.{createCDCSource, createKafkaSink}
import com.aws.analytics.conf.Config
import com.aws.analytics.model.DataModel
import com.google.gson.{GsonBuilder, JsonObject}
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, dataStreamConversions}
import org.apache.flink.table.api.{AnyWithOperations, FieldExpression, Schema, UnresolvedFieldExpression}
import org.apache.flink.table.expressions.PlannerExpressionParserImpl.proctime
import org.slf4j.LoggerFactory

import java.util.Properties


object KafkaTemporalJoin {

  private val log = LoggerFactory.getLogger(KafkaTemporalJoin.getClass)

  private val gson = new GsonBuilder().create

  def createKafkaSource(env: StreamExecutionEnvironment, parmas: Config): DataStream[String] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", parmas.brokerList)
    properties.setProperty("group.id", parmas.groupId)
    val myConsumer = new FlinkKafkaConsumer[String](parmas.sourceTopic, new SimpleStringSchema(), properties) // start from the earliest record possiblemyConsumer.setStartFromLatest()        // start from the latest record
    env.addSource(myConsumer)
  }
  def createCDCSource(params:Config): MySqlSource[String]={
    var startPos=StartupOptions.initial()
    if (params.position == "latest"){
      startPos= StartupOptions.latest()
    }

    MySqlSource.builder[String]
      .hostname(params.host.split(":")(0))
      .port(params.host.split(":")(1).toInt)
      .username(params.username)
      .password(params.pwd)
      .databaseList(params.dbList)
      .tableList(params.tbList)
      .startupOptions(startPos)
      .deserializer(new JsonDebeziumDeserializationSchema).build

  }

  implicit def map2Properties(map: Map[String, String]): java.util.Properties = {
    (new java.util.Properties /: map) { case (props, (k, v)) => props.put(k, v); props }
  }


  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val params = Config.parseConfig(KafkaTemporalJoin, args)
    env.enableCheckpointing(params.checkpointInterval.toInt * 1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    val rocksBackend: StateBackend = new RocksDBStateBackend(params.checkpointDir)
    env.setStateBackend(rocksBackend)

    val mysqlDimStream = env.fromSource(createCDCSource(params), WatermarkStrategy.noWatermarks(), "mysql cdc dim source")
      .setParallelism(params.parallel.toInt).map(line=>{
      val jsonObj= gson.fromJson(line, classOf[JsonObject])
      val db = jsonObj.get("source").getAsJsonObject.get("db").getAsString
      val table = jsonObj.get("source").getAsJsonObject.get("table").getAsString
      val afterData=jsonObj.get("after").getAsJsonObject.toString
      if (db=="cdc_test_db" && table=="test_tb_01"){
        gson.fromJson(afterData,classOf[DataModel.Table01])
      }else{
        DataModel.Table01("null","null","null","null")
      }
    })
    val tEnv = StreamTableEnvironment.create(env)
    val mysqlDimTable = mysqlDimStream.toTable(tEnv,$"id", $"name",$"create_time", $"modify_time",'proc_time.proctime)
    //    mysqlDimTable.printSchema()

    val dimTTF = mysqlDimTable.createTemporalTableFunction($"proc_time", $"id")
    tEnv.registerFunction("dimTTF",dimTTF)
//    tEnv.createTemporaryView("dim", mysqlDimTable)

    val source = createKafkaSource(env, params)

   val sourceTable= source.map(line=>{
        gson.fromJson(line,classOf[DataModel.Table02])
    }).toTable(tEnv,$"id", $"name",'proc_time.proctime)
    tEnv.createTemporaryView("source", sourceTable)

    val sql = tEnv.executeSql("select t1.*,t2.* from  source as t1, " +
      " Lateral table (dimTTF(t1.proc_time)) as t2 where t1.id=t2.id")

    log.info("print result ...")

    sql.print()

    tEnv.execute("temporal join demo")
  }


}
