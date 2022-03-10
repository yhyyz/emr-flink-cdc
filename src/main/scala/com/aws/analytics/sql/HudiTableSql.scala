package com.aws.analytics.sql

object HudiTableSql {
  def createTB1(sinkTB:String)={
      s"""CREATE TABLE  $sinkTB(
         |id string,
         |name string,
         |create_time string,
         |modify_time string,
         |logday VARCHAR(255),
         |hh VARCHAR(255)
         |)PARTITIONED BY (`logday`,`hh`)
         |WITH (
         |  'connector' = 'hudi',
         |  'path' = 's3a://app-util/cdc-data/$sinkTB/',
         |  'table.type' = 'COPY_ON_WRITE',
         |  'write.precombine.field' = 'modify_time',
         |  'write.operation' = 'upsert',
         |  'hoodie.datasource.write.recordkey.field' = 'id',
         |  'hive_sync.enable' = 'false',
         |  'hive_sync.use_jdbc' = 'false',
         |  'hive_sync.metastore.uris' = 'thrift://localhost:9083',
         |  'hive_sync.table' = '$sinkTB',
         |  'hive_sync.mode' = 'HMS',
         |  'hive_sync.username' = 'hadoop',
         |  'hive_sync.partition_fields' = 'logday,hh',
         |  'hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.MultiPartKeysValueExtractor'
         |  )
      """.stripMargin
  }
  def createTB2(sinkTB:String)={
    s"""CREATE TABLE  $sinkTB(
       |id string,
       |name string,
       |logday VARCHAR(255),
       |hh VARCHAR(255)
       |)PARTITIONED BY (`logday`,`hh`)
       |WITH (
       |  'connector' = 'hudi',
       |  'path' = 's3a://app-util/cdc-data/$sinkTB/',
       |  'table.type' = 'COPY_ON_WRITE',
       |  'write.precombine.field' = 'name',
       |  'write.operation' = 'upsert',
       |  'hoodie.datasource.write.recordkey.field' = 'id',
       |  'hive_sync.enable' = 'false',
       |  'hive_sync.use_jdbc' = 'false',
       |  'hive_sync.metastore.uris' = 'thrift://localhost:9083',
       |  'hive_sync.table' = '$sinkTB',
       |  'hive_sync.mode' = 'HMS',
       |  'hive_sync.username' = 'hadoop',
       |  'hive_sync.partition_fields' = 'logday,hh',
       |  'hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.MultiPartKeysValueExtractor'
       |  )
      """.stripMargin
  }
  def createTB3(sinkTB:String)={
    s"""CREATE TABLE  $sinkTB(
       |id string,
       |name string,
       |address string,
       |create_time string,
       |modify_time string,
       |logday VARCHAR(255),
       |hh VARCHAR(255)
       |)PARTITIONED BY (`logday`,`hh`)
       |WITH (
       |  'connector' = 'hudi',
       |  'path' = 's3a://app-util/cdc-data/$sinkTB/',
       |  'table.type' = 'COPY_ON_WRITE',
       |  'write.precombine.field' = 'modify_time',
       |  'write.operation' = 'upsert',
       |  'hoodie.datasource.write.recordkey.field' = 'id',
       |  'hive_sync.enable' = 'false',
       |  'hive_sync.use_jdbc' = 'false',
       |  'hive_sync.metastore.uris' = 'thrift://localhost:9083',
       |  'hive_sync.table' = '$sinkTB',
       |  'hive_sync.mode' = 'HMS',
       |  'hive_sync.username' = 'hadoop',
       |  'hive_sync.partition_fields' = 'logday,hh',
       |  'hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.MultiPartKeysValueExtractor'
       |  )
      """.stripMargin
  }

  def insertTBSQL(sinkTB:String,prefix:String)={
    s"""
       |insert into $sinkTB select *,DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') as logday, DATE_FORMAT(CURRENT_TIMESTAMP, 'hh') as hh  from $prefix$sinkTB
       |""".stripMargin
  }


}
