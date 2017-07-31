package Scala.com.zj.wordsplit

import java.util

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{DataFrame, SparkSession}
object SplitWord {
  def split(ss:SparkSession):DataFrame={
    val sc=ss.sqlContext
    import sc.implicits._
    val mongo_rdd = MongoSpark.load(ss.sparkContext).repartition(10)
    val splited_df:DataFrame=mongo_rdd.map(doc=>(doc.getString("weibo_id"),SplitTools.split(doc.getString("weibo_content")).toArray)).toDF("weibo_id","content")
    splited_df
  }
}
