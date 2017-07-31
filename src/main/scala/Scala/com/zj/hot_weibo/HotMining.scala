package Scala.com.zj.hot_weibo

import Scala.com.zj.dao.hot_weibo_dao.mysql.WeiboTopicDao

import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object HotMining {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName("hot mining").master("local[*]").getOrCreate()
    import ss.implicits._
    val sc =ss.sqlContext
    val url = "jdbc:mysql://172.17.11.173:3306/weibo?useUnicode=true&characterEncoding=UTF-8"
    val weibo_theme_df:DataFrame= sc.read.format( "jdbc" ).options(
      Map( "url" -> url,
        "user" -> "root",
        "password" -> "mbshqqb",
        "dbtable" -> "weibo_theme" )).load()
    weibo_theme_df.show()

    ss.udf.register("calc_hotdegree",inf_udaf)
    val theme_hot:DataFrame=weibo_theme_df.groupBy("theme_id").agg(Map("weibo_id"->"calc_hotdegree"))
    theme_hot.show()
    val theme_topics=sc.read.format( "jdbc" ).options(
      Map( "url" -> url,
        "user" -> "root",
        "password" -> "mbshqqb",
        "dbtable" -> "theme_t1_t2_t3" )).load()

    val topic_words=sc.read.format( "jdbc" ).options(
      Map( "url" -> url,
        "user" -> "root",
        "password" -> "mbshqqb",
        "dbtable" -> "topic_words" )).load()

    val theme_words=theme_topics.join(topic_words,theme_topics("topic1_id")===topic_words("topic_id")).select($"theme_id",$"topic2_id",$"topic3_id",$"topic_words".alias("topic1_words"))
      .join(topic_words,theme_topics("topic2_id")===topic_words("topic_id")).select($"theme_id",$"topic1_words",$"topic3_id",$"topic_words".alias("topic2_words"))
      .join(topic_words,theme_topics("topic3_id")===topic_words("topic_id")).select($"theme_id",$"topic1_words",$"topic2_words",$"topic_words".alias("topic3_words"))

    theme_hot.join(theme_words,"theme_id").sort($"calc_hotdegree(weibo_id)".desc).show()
  }
}
