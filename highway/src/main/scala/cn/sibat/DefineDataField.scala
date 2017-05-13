package cn.sibat

import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * 数据格式化
  * Created by wing1995 on 2017/5/13.
  */
class DefineDataField(data: Dataset[String]) {
  import data.sparkSession.implicits._
  /**
    * 默认将SZT刷卡产生的原始记录中的每一列贴上字段标签
    * 字段名称：0.记录编码 1.卡片逻辑编码 2.刷卡设备终端编码 3.公司编码 4.交易类型 5.交易金额 6.卡内余额
    * 7.未知字段 8.拍卡时间 9.成功标识 10.未知时间1 11.未知时间2 12.公司名称 13.站点名称 14.车辆编号
    * @return
    */
  def trans_SZT: DataFrame = {
    val SZT_df= data.map(_.split(","))
      .map(line => SZT(line(0), line(1), line(2), line(3), line(4), line(5).toDouble, line(6).toDouble,
        line(7), line(8), line(9), line(10), line(11), line(12), line(13), line(14)))
      .toDF()
    SZT_df
  }
}

class

object DefineDataField {
  def apply(data: Dataset[String]): DefineDataField = new DefineDataField(data)
}