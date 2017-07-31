package Scala.com.zj.utils

import java.sql.{PreparedStatement, ResultSet, ResultSetMetaData, SQLException, Statement}

import Scala.com.zj.dao.hot_weibo_dao.mysql.WeiBoWithThemeDao.sqlHelper

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by BigData on 2017/7/5.
  */
class SqlHelper {
  val conn = MySqlDBUtil.getConnection

  @throws[SQLException]
  def executeQuery(sql: String, params: Any*): List[Map[String, Any]] = {
    var pstmt: PreparedStatement = null
    var rs: ResultSet = null
    val listBuilder: mutable.Builder[Map[String, Any], List[Map[String, Any]]] = List.newBuilder[Map[String, Any]]
    // 占位符索引。（'?'的索引）
    var placeHolderIndex = 1
    pstmt = conn.prepareStatement(sql)
    params.foreach {
      case param: String =>{
        pstmt.setString(placeHolderIndex, param)
        placeHolderIndex += 1
      }
      case param =>
        pstmt.setObject(placeHolderIndex, param)
        placeHolderIndex += 1
    }
    rs = pstmt.executeQuery()
    val rsmd:ResultSetMetaData= rs.getMetaData
    while (rs.next()) {//每一行
      val mapBuilder:mutable.Builder[(String, Any), Map[String, Any]] = Map.newBuilder[String, Any]
      for (i <- 0 until rsmd.getColumnCount) {
        val name = rsmd.getColumnName(i+1)
        mapBuilder += name->rs.getObject(name)
      }
      listBuilder += mapBuilder.result()
    }
    this.close(pstmt, rs)
    listBuilder.result
  }

  @throws[SQLException]
  def executeBatch(sql: String, params: List[List[String]]):Array[Int]={
    conn.setAutoCommit(false)
    val pstmt = conn.prepareStatement(sql)
    params.foreach(rowParam=>{
      for(i<-0 until rowParam.size){
        pstmt.setObject(i + 1, rowParam(i))
      }
      pstmt.addBatch()
    })
    val result= pstmt.executeBatch
    conn.commit()
//    conn.setAutoCommit(true)
    result
  }

  @throws[SQLException]
  def executeUpdate(sql: String, params: Any*): Int = {
val pstmt = conn.prepareStatement(sql)
    var placeHolderIndex = 1
    params.foreach {
      case param: String =>
        param.split(",").map(_.trim).foreach(s => {
          pstmt.setString(placeHolderIndex, s)
          placeHolderIndex += 1
        })
      case param:StringBuffer=>
        pstmt.setObject(placeHolderIndex, param)
        placeHolderIndex += 1
      case param=>
        pstmt.setObject(placeHolderIndex, param)
        placeHolderIndex += 1
    }
    val result = pstmt.executeUpdate
    pstmt.close()
    return result
  }

  @throws[SQLException]
  def close(stmt: Statement, rs: ResultSet): Unit = {
    if (null != rs) rs.close()
    if (null != stmt) stmt.close()
  }

  @throws[SQLException]
  def setAutoCommit(flag: Boolean): Unit = {
    this.conn.setAutoCommit(flag)
  }

  @throws[SQLException]
  def commit(): Unit = {
    this.conn.commit()
  }

  @throws[SQLException]
  def rollBack(): Unit = {
    this.conn.rollback()
  }

  def closeConnection(): Unit ={
    MySqlDBUtil.closeConnection(conn)
  }

  /**
    * 生成多个?。用于生成带In的sql语句。
    *
    * @param length
    * @return
    */
  def preparePlaceHolders(length: Int): String = {
    val builder = new StringBuilder
    for(i <-0 until length){
      builder.append("?")
      if(i+1<length) builder.append(",")
    }
    builder.toString
  }

  @throws[SQLException]
  def main(args: Array[String]): Unit = {

  }
}
