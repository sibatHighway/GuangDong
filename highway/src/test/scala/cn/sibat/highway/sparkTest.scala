package cn.sibat.highway

import org.apache.spark.sql.SparkSession

/**
  * Created by wing1995 on 2017/5/13.
  */
object sparkTest {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:/file:E:/GuangDong")
      .appName("Spark SQL Test")
      .master("local[*]")
      .getOrCreate()

    //读取原始数据，格式化
    val ds = spark.read.textFile("E:\\GaoSuFormerCode\\201705\\sampleData.txt")
    val df = DefineDataField(ds).trans_df
    df.show(100)
  }
}