package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.model.DataModel
import com.aws.analytics.sql.HudiTableSql
import com.google.gson.GsonBuilder
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala.createTypeInformation
import org.apache.logging.log4j.LogManager
//import org.apache.logging.log4j.LogManager
import com.google.gson.JsonObject
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.util.Collector

import java.util.Properties


  object Kafka2Hudi{

    private val log = LogManager.getLogger(Kafka2Hudi.getClass)
    private val gson = new GsonBuilder().create

    def createKafkaSource(env: StreamExecutionEnvironment, parmas: Config): DataStream[String] = {
      val properties = new Properties()
      properties.setProperty("bootstrap.servers", parmas.brokerList)
      properties.setProperty("group.id", parmas.groupId)
      val myConsumer = new FlinkKafkaConsumer[String](parmas.sourceTopic, new SimpleStringSchema(), properties) // start from the earliest record possiblemyConsumer.setStartFromLatest()        // start from the latest record
      env.addSource(myConsumer)
    }

    def main(args: Array[String]) {
      val parmas = Config.parseConfig(Kafka2Hudi, args)
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.enableCheckpointing(parmas.checkpointInterval.toInt * 1000)
      env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
      env.getCheckpointConfig.setCheckpointTimeout(60000)
      env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
      val rocksBackend: StateBackend = new RocksDBStateBackend(parmas.checkpointDir)
      env.setStateBackend(rocksBackend)

      val settings = EnvironmentSettings.newInstance.useBlinkPlanner().inStreamingMode().build()
      val tEnv = StreamTableEnvironment.create(env, settings)
      val conf = new Configuration()
      conf.setString("pipeline.name","kafka-mysql-cdc-hudi");
      tEnv.getConfig.addConfiguration(conf)
      // create kafka source
      val source = createKafkaSource(env, parmas)

      val sinkTB1= "test_tb_union"
      val sinkTB2= "test_tb_no_time"
      val sinkTB3= "dev_tb_01"
      val s3Path = "s3a://app-util/cdc-data-01"

      val ot1 = OutputTag[DataModel.Table01](sinkTB1)
      val ot2 = OutputTag[DataModel.Table02](sinkTB2)
      val ot3 = OutputTag[DataModel.Table03](sinkTB3)

      val outputStream = source.process(new ProcessFunction[String, String] {
        override def processElement(
                                     value: String,
                                     ctx: ProcessFunction[String, String]#Context,
                                     out: Collector[String]): Unit = {
          try {
          val jsonObj= gson.fromJson(value, classOf[JsonObject])
          val db = jsonObj.get("source").getAsJsonObject.get("db").getAsString
          val table = jsonObj.get("source").getAsJsonObject.get("table").getAsString
          val afterData=jsonObj.get("after").getAsJsonObject.toString
          db match {
            case "cdc_test_db" =>
              table match {
                case "test_tb_01" | "test_tb_02" =>  ctx.output(ot1, gson.fromJson(afterData,classOf[DataModel.Table01]))
                case "test_tb_no_time" =>  ctx.output(ot2,gson.fromJson(afterData,classOf[DataModel.Table02]))
              }
            case "cdc_dev_db" =>
              table match {
                case "dev_tb_01"  =>  ctx.output(ot3,gson.fromJson(afterData,classOf[DataModel.Table03]))
              }
            case _ => out.collect(value)
          }
        }catch {
          case e: Exception => {
            log.error(e.getMessage)
          }
        }}
      })

      val prefix="source_"
      val ot1Table = tEnv.fromDataStream(outputStream.getSideOutput(ot1))
      tEnv.createTemporaryView(prefix+sinkTB1, ot1Table)

      val ot2Table = tEnv.fromDataStream(outputStream.getSideOutput(ot2))
      tEnv.createTemporaryView(prefix+sinkTB2, ot2Table)

      val ot3Table = tEnv.fromDataStream(outputStream.getSideOutput(ot3))
      tEnv.createTemporaryView(prefix+sinkTB3, ot3Table)

      tEnv.executeSql(HudiTableSql.createTB1(s3Path,sinkTB1))
      tEnv.executeSql(HudiTableSql.createTB2(s3Path,sinkTB2))
      tEnv.executeSql(HudiTableSql.createTB3(s3Path,sinkTB3))

      val stat =  tEnv.createStatementSet()
      stat.addInsertSql(HudiTableSql.insertTBSQL(sinkTB1,prefix))
      stat.addInsertSql(HudiTableSql.insertTBSQL(sinkTB2,prefix))
      stat.addInsertSql(HudiTableSql.insertTBSQL(sinkTB3,prefix))
      stat.execute()

    }
  }