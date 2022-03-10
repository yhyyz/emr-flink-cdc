package com.aws.analytics.model

object DataModel {

  case class Table01(id: String, name: String,create_time:String,modify_time:String)
  case class Table02(id: String, name: String)
  case class Table03(id: String, name: String,address:String, create_time:String,modify_time:String)


}
