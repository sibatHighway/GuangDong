package cn.sibat.highway

import org.apache.spark.sql.SparkSession

/**
  * 清洗数据的数据测试
  * Created by wing1995 on 2017/5/13.
  */
object CleanDataTest {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:/file:E:/GuangDong/201601")
      .appName("Spark SQL Test")
      .master("local[*]")
      .getOrCreate()

    //读取原始数据，格式化
    val ds = spark.read.textFile("E:\\GaoSuFormerCode\\201705\\201601.txt")
  }
}
