package cn.sibat.highway

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame}

/**
  * 清洗收费站打卡数据
  * Created by wing1995 on 2017/5/13.
  */
class CleanDataUtils(data: DataFrame) {
  def toDF: DataFrame = this.data

  private def newUtils(df: DataFrame): CleanDataUtils = new CleanDataUtils(df)

//  def removeMark(): CleanDataUtils = {
//    val normalData = data.columns.map(c => col(c))
//  }
}

object CleanDataUtils {
  def apply(data: DataFrame): CleanDataUtils = new CleanDataUtils(data)
}
