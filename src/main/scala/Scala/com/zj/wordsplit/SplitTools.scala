package Scala.com.zj.wordsplit

import java.io.{IOException, StringReader}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.lionsoul.jcseg.tokenizer.ASegment
import org.lionsoul.jcseg.tokenizer.core._

import scala.collection.mutable

object SplitTools {
  def split (line: String):List[String]={
    val config:JcsegTaskConfig= new JcsegTaskConfig(SplitTools.getClass.getResource("/").getPath + "jcseg.properties")
    val dic:ADictionary = DictionaryFactory.createDefaultDictionary(config)
    val listBuilder:mutable.Builder[String, List[String]]=List.newBuilder[String]
    import java.util.regex.Pattern
    val regex = "^\\u200b{1,5}$"
    val p = Pattern.compile(regex) //获取正则表达式中的分组，每一组小括号为一组

    try {
      val seg = SegmentFactory.createJcseg(JcsegTaskConfig.COMPLEX_MODE,config, dic)
      seg.reset(new StringReader(line))
      var word = seg.next
      while (word != null) {
        val m = p.matcher(word.getValue)
        if(word.getValue.trim.length>0 && !m.find()){
          listBuilder += word.getValue.trim
        }
        word = seg.next
      }
    }catch {
      case e1: JcsegException =>
        e1.printStackTrace()
      case e2: IOException=>
        e2.printStackTrace()
    }
    listBuilder.result()
  }

  def main(args: Array[String]) = {
    val str = "!@##$%$%^&*&*(HBase中通过row和columns确定的为一个存贮单元称为cell。显示每个元素，每个 cell都保存着同一份数据的多个版本。版本通过时间戳来索引。迎泽区是繁华的地方,营业厅营业"
    split(str).foreach(println)
  }
}
