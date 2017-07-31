package Scala.com.zj.dao.hot_weibo_dao.mongo

import java.text.SimpleDateFormat

import Scala.com.zj.hot_weibo.WeiBoWithThemeVO
import Scala.com.zj.utils.{MongoHelper, SqlHelper}
import com.mongodb.casbah.query.Imports._

import scala.collection.mutable

object WeiBoWithThemeDao {
  val mongoHelper=new MongoHelper()
  def getWeiBoWithTheme(weibo_id: String):WeiBoWithThemeVO={
    try {
      val queryWeiboMap = mutable.HashMap[String, AnyRef]("weibo_id" -> weibo_id)
      val projectionMap = mutable.HashMap[String, Int]()
      val row_w1 = mongoHelper.findOne("weibo_info", queryWeiboMap, projectionMap)
      /** *****************************************************************************************/
      val user_id: String = row_w1.getOrElse("user_id", "0").toString
      val time_1: String = row_w1.getOrElse("weibo_time", "0").toString

      var forward_str_1: String = row_w1.getOrElse("forward_number", "0").toString
      if (forward_str_1.length > 2) {
        forward_str_1 = forward_str_1.substring(3, forward_str_1.length - 1)
      }else{
        forward_str_1="0"
      }
      var comment_str_1: String = row_w1.getOrElse("comment_number", "0").toString
      if (comment_str_1.length > 2) {
        comment_str_1 = comment_str_1.substring(3, comment_str_1.length - 1)
      }else{
        comment_str_1="0"
      }
      var thumbup_str_1: String = row_w1.getOrElse("thumbup_number", "0").toString
      if (thumbup_str_1.length > 1) {
        thumbup_str_1 = thumbup_str_1.substring(2, thumbup_str_1.length - 1)
      }else{
        thumbup_str_1="0"
      }
      /** *********************************************************************************/
      val row_w2 = mongoHelper.findOne("weibo_info", queryWeiboMap, projectionMap)
      val time_2: String = row_w2.getOrElse("weibo_time", "0").toString
      var forward_str_2: String = row_w2.getOrElse("forward_number", "0").toString
      if (forward_str_2.length > 2) {
        forward_str_2 = forward_str_2.substring(3, forward_str_2.length - 1)
      }else{
        forward_str_2="0"
      }
      var comment_str_2: String = row_w2.getOrElse("comment_number", "0").toString
      if (comment_str_2.length > 2) {
        comment_str_2 = comment_str_2.substring(3, comment_str_2.length - 1)
      }else{
        comment_str_2="0"
      }
      var thumbup_str_2: String = row_w2.getOrElse("thumbup_number", "0").toString
      if (thumbup_str_2.length > 1) {
        thumbup_str_2 = thumbup_str_2.substring(2, thumbup_str_2.length - 1)
      }else{
        thumbup_str_2="0"
      }
      /** ****************************************************************************************/
      val queryUserMap = mutable.HashMap[String, AnyRef]("user_id" -> user_id)
      val row_u = mongoHelper.findOne("user_info", queryUserMap, projectionMap)
      var weibo_str: String = row_u.getOrElse("weibo_number", "0").toString
      if (weibo_str.length > 2) {
        weibo_str = weibo_str.substring(3, weibo_str.length - 1)
      }else{
        weibo_str="0"
      }
      var fans_str: String = row_u.getOrElse("fans_number", "0").toString
      if (fans_str.length > 2) {
        fans_str = fans_str.substring(3, fans_str.length - 1)
      }else{
        fans_str="0"
      }
      var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-DD HH:mm:ss")
//println(user_id, fans_str, weibo_str, weibo_id, time_1, comment_str_1,
//  forward_str_1, thumbup_str_1, time_2, comment_str_2, forward_str_2, thumbup_str_2)
      return WeiBoWithThemeVO(user_id, fans_str.toDouble, weibo_str.toDouble, weibo_id, df.parse(time_1), comment_str_1.toDouble,
        forward_str_1.toDouble, thumbup_str_1.toDouble, df.parse(time_2), comment_str_2.toDouble, forward_str_2.toDouble, thumbup_str_2.toDouble)
    }catch{
      case e=>{
        println(e)
        null
      }
    }
  }
  def main(args: Array[String]): Unit = {
//    getWeiBoWithTheme()
  }
}
