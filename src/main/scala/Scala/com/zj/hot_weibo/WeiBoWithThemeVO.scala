package Scala.com.zj.hot_weibo

import java.util.Date

case class WeiBoWithThemeVO(
    user_id:String,
    fans_number:Double,
    weibo_number:Double,
    weibo_id:String,
    time_1:Date,
    comment_number_1:Double,
    forward_number_1:Double,
    thumbup_number_1:Double,

    time_2:Date,
    comment_number_2:Double,
    forward_number_2:Double,
    thumbup_number_2:Double) {

}
