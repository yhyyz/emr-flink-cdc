package com.aws.analytics


import com.aws.analytics.conf.Config
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants

import java.util.Properties

object CDC2KDS {
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

  def createKinesisSink(params:Config)={
    val producerConfig = new Properties()
    // Required configs
    producerConfig.put(AWSConfigConstants.AWS_REGION,params.kdsRegion)
    //    producerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id")
    //    producerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key")
    // Optional KPL configs
    producerConfig.put("AggregationMaxCount", "4294967295")
    producerConfig.put("CollectionMaxCount", "500")
    producerConfig.put("RecordTtl", "30000")
    producerConfig.put("RequestTimeout", "6000")
    producerConfig.put("ThreadPoolSize", "15")
    // Disable Aggregation if it's not supported by a consumer
    producerConfig.put("AggregationEnabled", "false")
    // Switch KinesisProducer's threading model
    // producerConfig.put("ThreadingModel", "PER_REQUEST")

    val kinesis = new FlinkKinesisProducer[String](new SimpleStringSchema, producerConfig)
    kinesis.setFailOnError(true)
    kinesis.setDefaultStream(params.kdsName)
    kinesis.setDefaultPartition("0")

    kinesis
  }



  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val params = Config.parseConfig(CDC2KDS, args)
    env.enableCheckpointing(params.checkpointInterval.toInt * 1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    val rocksBackend: StateBackend = new RocksDBStateBackend(params.checkpointDir)
    env.setStateBackend(rocksBackend)

    env.fromSource(createCDCSource(params), WatermarkStrategy.noWatermarks(), "mysql cdc source")
      .addSink(createKinesisSink(params)).name("kinesis sink")
      .setParallelism(params.parallel.toInt)
    env.execute("MySQL Binlog CDC Kinesis")
  }

}

