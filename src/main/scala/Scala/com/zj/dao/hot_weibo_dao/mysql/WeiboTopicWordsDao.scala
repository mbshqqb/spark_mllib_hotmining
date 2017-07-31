package Scala.com.zj.dao.hot_weibo_dao.mysql

import Scala.com.zj.utils.{MongoHelper, SqlHelper}
import org.apache.spark.ml.linalg
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

object WeiboTopicWordsDao {
  val sqlHelper:SqlHelper=new SqlHelper()

  def saveWeiboToTopic(weibo_topic:DataFrame): Unit ={

  }
  def saveTopicToWord(topic_words:DataFrame): Unit ={

  }
  def saveTopicClusterCenters(clusterCenters:Array[linalg.Vector],vocabulary:Array[String]): Unit ={
    val sql="insert into topic(topic_id,topic_words)values(?,?)"
    val listBuilder=List.newBuilder[List[String]]
    val batch:ArrayBuffer[List[String]]=new ArrayBuffer[List[String]]()
//    for(i<-0 until clusterCenters.length){
//
//      val row=clusterCenters(i).toArray.map(vocabulary(_.toInt)).map(t=>t)
//      listBuilder += row
//      println(row)
//    }
//    sqlHelper.executeBatch(sql,listBuilder.result)
//    sqlHelper.executeBatch()
  }
}
