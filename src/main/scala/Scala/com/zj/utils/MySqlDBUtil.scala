package Scala.com.zj.utils

import java.io.InputStream
import java.sql.{Connection, DriverManager, ResultSet, SQLException, Statement}
import java.util.Properties

/**
  * Created by BigData on 2017/7/5.
  */
object MySqlDBUtil {
  val is: InputStream = this.getClass.getClassLoader.getResourceAsStream("mysqldb.properties")
  val prop=new Properties()
  prop.load(is)
  val url = prop.getProperty("url")
  val user = prop.getProperty("user")
  val driver = prop.getProperty("driver")
  val password = prop.getProperty("password")
  Class.forName(driver)
  def getProperties(): Properties ={
    prop
  }
  def getConnection():Connection={
    try{
      DriverManager.getConnection(url, user, password)
    }catch {
      case e: SQLException=>throw e
    }
  }
  def closeConnection(conn: Connection): Unit ={
    if (conn != null)
      try
        conn.close()
      catch {
        case e: SQLException =>
          e.printStackTrace()
      }
  }

  @throws[SQLException]
  def main(args: Array[String]): Unit = {
    val conn = this.getConnection
    val stmt = conn.createStatement
    val rs = stmt.executeQuery("SELECT * FROM users")
    while ( {
      rs.next
    }) System.out.println(rs.getString(1) + "," + rs.getString(2))
  }
}
