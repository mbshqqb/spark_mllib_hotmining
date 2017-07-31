package Scala.com.zj.dao.hot_weibo_dao.mysql

import java.text.SimpleDateFormat

import Scala.com.zj.hot_weibo.WeiBoWithThemeVO
import Scala.com.zj.utils.SqlHelper

object WeiBoWithThemeDao {
  val sqlHelper=new SqlHelper()
  def getWeiBoWithTheme(weibo_id: String):WeiBoWithThemeVO={
    val sql_weibo_1="select user_id,weibo_time,forward_number,comment_number,thumbup_number from weibo_info where weibo_id=?"
    val rows_w1:List[Map[String, Any]]=sqlHelper.executeQuery(sql_weibo_1,weibo_id)
    val row_w1=rows_w1(0)
    val user_id:String=row_w1.getOrElse("user_id","0").toString

    val time_1:String=row_w1.getOrElse("weibo_time","0").toString

    var forward_str_1:Array[String]=row_w1.getOrElse("forward_number","0").toString.split("转发")
    print("转发=")
    if(forward_str_1.length==1){
      println(forward_str_1(0))
    }else{
      println(0)
    }
    println(forward_str_1)
    var comment_str_1:Array[String]=row_w1.getOrElse("comment_number","0").toString.split("评论")
    print("评论=")
    if(comment_str_1.length==1){
      println(comment_str_1(0))
    }else{
      println(0)
    }
    println(comment_str_1)
    var thumbup_str_1:Array[String]=row_w1.getOrElse("thumbup_number","0").toString.split("赞")
    print("赞=")
    if(thumbup_str_1.length==1){
      println(thumbup_str_1(0))
    }else{
      println(0)
    }
    println(thumbup_str_1)
    val sql_weibo_2="select weibo_id,weibo_time,forward_number,comment_number,thumbup_number from weibo_info_2 where weibo_id=?"
    val rows_w2:List[Map[String, Any]]=sqlHelper.executeQuery(sql_weibo_2,weibo_id)
    val row_w2=rows_w2(0)
    val time_2:String=row_w2.getOrElse("weibo_time","0").toString
    var forward_str_2:Array[String]=row_w2.getOrElse("comment_number","0").toString.split("转发")
    print("转发=")
    if(forward_str_2.length==1){
      println(forward_str_2(0))
    }else{
      println(0)
    }
    println(forward_str_2)
    var comment_str_2:Array[String]=row_w2.getOrElse("forward_number","0").toString.split("评论")
    print("评论=")
    if(comment_str_2.length==1){
      println(comment_str_2(0))
    }else{
      println(0)
    }
    println(comment_str_2)
    var thumbup_str_2:Array[String]=row_w2.getOrElse("thumbup_number","0").toString.split("赞")
    print("赞=")
    if(thumbup_str_2.length==1){
      println(thumbup_str_2(0))
    }else{
      println(0)
    }
    println(thumbup_str_2)
    val sql_user="select weibo_number,fans_number from user_info where user_id=?"
    val rows_u:List[Map[String, Any]]=sqlHelper.executeQuery(sql_user,user_id)
    val row_u=rows_u(0)
    var weibo_str:Array[String]=row_u.getOrElse("weibo_number","0").toString.split("微博")
    print("微博=")
    if(weibo_str.length==1){
      println(weibo_str(0))
    }else{
      println(0)
    }
    println(weibo_str)
    var fans_str:Array[String]=row_u.getOrElse("fans_number","0").toString.split("粉丝")
    print("粉丝=")
    if(fans_str.length==1){
      println(fans_str(0))
    }else{
      println(0)
    }
    println(fans_str)
    var df:SimpleDateFormat=new SimpleDateFormat("yyyy-MM-DD HH:mm:ss")

    return WeiBoWithThemeVO(user_id,1.toDouble,1.toDouble,weibo_id,df.parse(time_1),1.toDouble,
      1.toDouble,1.toDouble,df.parse(time_2),1.toDouble,1.toDouble,1.toDouble)
//    return WeiBoWithThemeVO(user_id,fans_number.toDouble,weibo_number.toDouble,weibo_id,df.parse(time_1),comment_number_1.toDouble,
//      forward_number_1.toDouble,thumbup_number_1.toDouble,df.parse(time_2),comment_number_2.toDouble,forward_number_2.toDouble,thumbup_number_2.toDouble)
  }
  def main(args: Array[String]): Unit = {

  }
}
