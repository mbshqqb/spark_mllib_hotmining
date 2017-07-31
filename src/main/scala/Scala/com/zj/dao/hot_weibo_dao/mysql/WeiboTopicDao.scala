package Scala.com.zj.dao.hot_weibo_dao.mysql

import WeiBoWithThemeDao.sqlHelper
import Scala.com.zj.utils.SqlHelper

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object WeiboTopicDao{
  val sqlHelper=new SqlHelper()
  def getTopicByWeiboId(weibo_id: String):Double ={
    val sql_topic="select cluster from kmeans_result where weibo_id=?"
    val rows_t:List[Map[String, Any]]=sqlHelper.executeQuery(sql_topic,weibo_id)
    val row_t=rows_t(0)
    val cluster:String=row_t.getOrElse("cluster","-1").toString
    return cluster.toDouble
  }
  def getWeiboIdsByTopic(cluster: String):List[String]={
    val sql_weibos="select weibo_id from kmeans_result where cluster=?"
    val rows_t:List[Map[String, Any]]=sqlHelper.executeQuery(sql_weibos,cluster)
    val weibo_ids_builder=List.newBuilder[String]
    for(row<-rows_t){
      weibo_ids_builder +=row.getOrElse("weibo_id","0").toString
    }
    return weibo_ids_builder.result()
  }
  def getAllTopic():List[String]={
    val sql_weibos="select cluster from kmeans_result"
    val rows_t:List[Map[String, Any]]=sqlHelper.executeQuery(sql_weibos)
    val topic_builder=List.newBuilder[String]
    for(row<-rows_t){
      topic_builder +=row.getOrElse("cluster","0").toString
    }
    return topic_builder.result()
  }
}
