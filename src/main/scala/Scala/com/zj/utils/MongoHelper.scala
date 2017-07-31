package Scala.com.zj.utils

import com.mongodb.DBCollection
import com.mongodb.casbah.Imports
import com.mongodb.casbah.Imports._

import scala.collection.mutable
/**
  * Created by BigData on 2017/7/27.
  */
class MongoHelper {

  val db:MongoDB=MongoDBUtil.getDB()

  def find(table:String, where:mutable.Map[String,AnyRef], projection:mutable.Map[String,Int]):List[DBObject]={
    val collection:MongoCollection=db.apply(table)
    val dBCursor:MongoCursor=collection.find(MongoDBObject(where.toList),MongoDBObject(projection.toList))
    dBCursor.toList
  }
  def findOne(table:String,where:mutable.Map[String,AnyRef], projection:mutable.Map[String,Int]):DBObject={
    val collection:DBCollection=db.getCollection(table)
    val mongoDBObject:DBObject=collection.findOne(MongoDBObject(where.toList),MongoDBObject(projection.toList))
    mongoDBObject
  }
  /**
    * 支持单条或者多条删除
    * @param where 对查询到的行进行删除 类型map key为查询的字段名，value为具体数值 (目前支持相等查询)
    *
    */
  def delete(table:String,where: mutable.Map[String,AnyRef])={
    val collection:MongoCollection=db.apply(table)
      collection.findAndRemove(DBObject.apply(where.toList))
  }

  /**
    * 目前支持单条数据插入
    * @param data 类型map key为列名，value为列的数值，即数据
    * @return
    */
  def insert(table:String,data:mutable.Map[String,AnyRef])={
    val collection:MongoCollection=db.apply(table)
      collection.insert(DBObject.apply(data.toList))
  }

  /**
    *
    * @param where 查询的行 类型map key为查询的字段名，value为具体数值
    * @param update 要更新行的数据 类型map key为跟新的字段名,value为具体数值
    * @param upsert True如果查询结果不存在，就直接跟新。
    * @return
    */
  def update(table:String,where: mutable.Map[String,AnyRef],update:mutable.Map[String,AnyRef],upsert:Boolean)={
    //如果不存在就直接插入
    val collection:MongoCollection=db.apply(table)
    collection.update(DBObject.apply(where.toList),DBObject.apply(update.toList),upsert = upsert)
  }

  def main(args: Array[String]): Unit = {
    val a:Imports.DBObject=findOne("weibo_info",mutable.Map[String,AnyRef]("weibo_id"->"Fem39mUl7"),mutable.Map[String,Int]())
    println(a.getOrElse("weibo_content",1))
    val b=find("weibo_info",mutable.Map[String,AnyRef]("weibo_id"->"Fem39mUl7"),mutable.Map[String,Int]())
    for(obj<-b){
      println(obj.getOrElse("weibo_content",1))
    }
  }
}
