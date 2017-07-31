package Scala.com.zj

import Scala.com.zj.dao.hot_weibo_dao.mysql.WeiboTopicWordsDao
import Scala.com.zj.hot_weibo.HotMining
import Scala.com.zj.thememining.ThemeMining
import Scala.com.zj.utils.MySqlDBUtil
import Scala.com.zj.wordsplit.SplitWord
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .config("spark.driver.extraJavaOptions","-Xms1024m -Xmx1024m -XX:PermSize=1024M -XX:MaxPermSize=1024M")
      .config("spark.mongodb.input.uri", "mongodb://root:root@172.17.11.169:27017")
      .config("spark.mongodb.input.database","zj")
      .config("spark.mongodb.input.collection","weibo_info")
      .config("spark.mongodb.input.readPreference.name","primaryPreferred")
      .getOrCreate()
    println("分词：")
    val content_df:DataFrame=SplitWord.split(ss)
    content_df.show(10,false)
    val (weibo_theme,theme_t1_t2_t3,topic_words):(DataFrame, DataFrame, DataFrame)=ThemeMining.tfidf(ss,content_df)
    val prop =MySqlDBUtil.getProperties()

    weibo_theme.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://172.17.11.173:3306/weibo?useUnicode=true&characterEncoding=UTF-8","weibo_theme",prop) // 表可以不存在
    theme_t1_t2_t3.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://172.17.11.173:3306/weibo?useUnicode=true&characterEncoding=UTF-8","theme_t1_t2_t3",prop) // 表可以不存在
    topic_words.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://172.17.11.173:3306/weibo?useUnicode=true&characterEncoding=UTF-8","topic_words",prop) // 表可以不存在

  }
}