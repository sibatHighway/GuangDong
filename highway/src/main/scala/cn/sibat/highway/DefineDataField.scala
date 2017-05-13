package cn.sibat.highway

import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * 数据格式化
  * Created by wing1995 on 2017/5/13.
  */
class DefineDataField(data: Dataset[String]) {
  import data.sparkSession.implicits._
  /**
    * 默认将高速收费产生的原始记录中的每一列贴上字段标签
    * 字段名称：入口区域编码、入口路段号、入口站编码、入口车道号、入口车道类型、入口发卡时间、区域编码、路段号、站编码、出口车道号、
    * 出口车道类型、收费时间、车牌号、车型、车种、公里、总轴数、轴型代码、总重量、限重、超限率、免费类型代码、路径标识、OBU号、
    * 是否绿色通道车辆代码、支付方式、车流量、出口流水号、上传时间
    * @return df
    */
  def trans_df: DataFrame = {
    val df= data.map(_.split(","))
      .map(line => highway(line(0), line(1), line(2), line(3), line(4), line(5).substring(1, line(5).length - 1), line(6), line(7), line(8), line(9), line(10),
        line(11).substring(1, line(11).length - 1), line(12).substring(1, line(12).length - 1).trim, line(13), line(14), line(15), line(16),
        line(17).substring(1, line(17).length - 1).trim, line(18), line(19), line(20), line(21), line(22).substring(1, line(22).length - 1).trim,
        line(23).substring(1, line(23).length - 1).trim, line(24), line(25), line(26), line(27).substring(1, line(27).length - 1), line(28).substring(1, line(28).length - 1)))
      .toDF()
    df
  }
}

case class highway (
                     InAreaNo: String, InRoadNo: String, InStationNo: String, inLaneNo: String, InLaneType: String, InOpTime: String,
                     AreaNo: String, RoadNo: String, StationNo: String, LaneNo: String, LaneType: String, OpTime: String,
                     VehPlate: String, VehType: String, VehKindFlag: String, Miles: String, AxisCnt: String, AxisCode: String,
                     AllWeight: String, LimitWeight: String, ExLimitRatio: String, FreeType: String, FlagStation: String, OBUNo: String,
                     IsGreen: String, PayType: String, VehCount: String, ListNo: String, UploadTime: String
                   )

object DefineDataField {
  def apply(data: Dataset[String]): DefineDataField = new DefineDataField(data)
}