package Scala.com.zj.hot_weibo

import Scala.com.zj.dao.hot_weibo_dao.mongo.WeiBoWithThemeDao
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
object inf_udaf extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(StructField("weibo_id", StringType) :: Nil)

  override def bufferSchema: StructType =  StructType(
    StructField("hot_degree", DoubleType) :: Nil)

  override def dataType: DataType =  DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0.toDouble
  }
  //每来一行我们计算热度值
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val weibo_id=input.getString(0)
    val weiBoWithThemeVO: WeiBoWithThemeVO=WeiBoWithThemeDao.getWeiBoWithTheme(weibo_id)
    if(weiBoWithThemeVO!=null){
      buffer(0)=buffer.getDouble(0)+inf_t(weiBoWithThemeVO)
    }else{
      println("null weiBoWithThemeVO")
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getDouble(0)+buffer2.getDouble(0)
  }
  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(0)
  }
  def inf_t(weiBoWithThemeVO: WeiBoWithThemeVO):Double={
    val a=100
    val b=20
    if(weiBoWithThemeVO.weibo_number==0){
      return 0
    }
    val infD_t:Double=weiBoWithThemeVO.weibo_number/weiBoWithThemeVO.fans_number
//    val infI_t:Double=(a*(weiBoWithThemeVO.comment_number_1-weiBoWithThemeVO.comment_number_2)+
//      b*(weiBoWithThemeVO.forward_number_1-weiBoWithThemeVO.forward_number_2))/(weiBoWithThemeVO.time_2.getTime-weiBoWithThemeVO.time_1.getTime+1)/1000
val infI_t:Double=(a*(weiBoWithThemeVO.comment_number_1-weiBoWithThemeVO.comment_number_2)+
  b*(weiBoWithThemeVO.forward_number_1-weiBoWithThemeVO.forward_number_2))/1000
    if(infI_t==Double.NaN){
      infD_t
    }
    infD_t+infI_t
  }
}