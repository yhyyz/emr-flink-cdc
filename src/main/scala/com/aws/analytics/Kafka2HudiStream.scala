//package com.aws.analytics
//
//import com.aws.analytics.conf.Config
//import com.aws.analytics.model.DataModel
//import com.aws.analytics.sql.HudiTableSql
//import com.google.gson.GsonBuilder
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
//import org.apache.flink.runtime.state.StateBackend
//import org.apache.flink.streaming.api.CheckpointingMode
//import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
//import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//import org.apache.flink.api.scala.createTypeInformation
//import org.apache.flink.formats.json.JsonRowDataDeserializationSchema
//import org.apache.flink.formats.json.debezium.DebeziumJsonDeserializationSchema
//import org.apache.flink.table.data.GenericRowData
//import org.apache.logging.log4j.LogManager
////import org.apache.logging.log4j.LogManager
//import com.google.gson.JsonObject
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.ProcessFunction
//import org.apache.flink.table.api.EnvironmentSettings
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
//import org.apache.flink.util.Collector
//import scala.collection.JavaConversions._
//
//import java.util.Properties
//
//
//object Kafka2HudiStream{
//
//  private val log = LogManager.getLogger(Kafka2Hudi.getClass)
//  private val gson = new GsonBuilder().create
//
//  def createKafkaSource(env: StreamExecutionEnvironment, parmas: Config): DataStream[String] = {
//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers", parmas.brokerList)
//    properties.setProperty("group.id", parmas.groupId)
//    val myConsumer = new FlinkKafkaConsumer[String](parmas.sourceTopic,
//      new JsonRowDataDeserializationSchema(), properties) // start from the earliest record possiblemyConsumer.setStartFromLatest()        // start from the latest record
//    env.addSource(myConsumer)
//  }
//
//  def main(args: Array[String]) {
//    val parmas = Config.parseConfig(Kafka2Hudi, args)
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.enableCheckpointing(parmas.checkpointInterval.toInt * 1000)
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
//    env.getCheckpointConfig.setCheckpointTimeout(60000)
//    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
//    val rocksBackend: StateBackend = new RocksDBStateBackend(parmas.checkpointDir)
//    env.setStateBackend(rocksBackend)
//
//    val settings = EnvironmentSettings.newInstance.useBlinkPlanner().inStreamingMode().build()
//    val tEnv = StreamTableEnvironment.create(env, settings)
//    val conf = new Configuration()
//    conf.setString("pipeline.name","kafka-mysql-cdc-hudi");
//    tEnv.getConfig.addConfiguration(conf)
//    // create kafka source
//    val source = createKafkaSource(env, parmas)
//
//    source.map(line=> {
//
//      val jsonObj= gson.fromJson(line, classOf[JsonObject])
//      val db = jsonObj.get("source").getAsJsonObject.get("db").getAsString
//      val table = jsonObj.get("source").getAsJsonObject.get("table").getAsString
//      val afterData=jsonObj.get("after").getAsJsonObject
//      val entries = afterData.entrySet
//      val row = new GenericRowData(afterData.entrySet.size()+2)
//      row.setField(0,db)
//      for(entry <- entries){
//        entry.getKey
//        entry.getValue
//      }
//      row
//
//    })
//
//
//
//
//  }
//}