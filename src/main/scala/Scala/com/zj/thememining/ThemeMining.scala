package Scala.com.zj.thememining

import org.apache.spark.ml.clustering.{KMeans, KMeansModel, LDA}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
object ThemeMining {
  def tfidf(ss:SparkSession, contentDF:DataFrame) = {
    import ss.implicits._

/***********************************计算tf有三种方式，一种方式是HashingTF，另一种方式是CountVectorizer，另一种的是CountVectorizerModel**************************************************/
//    //构建tf模型
//    val hashingTF = new HashingTF()
//      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(16)
//    //计算tf
//    val featurizedData = hashingTF.transform(wordsData)
//    featurizedData.show(false)
/*****************************************************************************/
//val cvModel = new CountVectorizerModel(Array("a", "b", "c"))
//  .setInputCol("words")
//  .setOutputCol("features")
/**************************************************************************************************************************************************************************************/
println("训练tf：")
val cvModel: CountVectorizerModel = new CountVectorizer()
    .setInputCol("content")
    .setOutputCol("count")
    .setVocabSize(5000)
    .setMinTF(1.2)
    .setMinDF(5)
    .fit(contentDF)

    //词汇将存储在 cvModel.vocabulary 中
    println("输出词典：")
    val vocabulary:Array[String]=cvModel.vocabulary
    //vocabulary.foreach(println(_))

    println("计算tf：")
    val tf_df=cvModel.transform(contentDF)
    tf_df.show(10,true)
    //构建idf模型
    println("训练tfidf：")
    val idfModel = new IDF()
        .setMinDocFreq(5)
      .setInputCol("count")
      .setOutputCol("tfidf")
      .fit(tf_df)
    //使用训练好的模型计算TF-IDF
    println("计算tf-idf：")
    val tfidf_df:DataFrame = idfModel.transform(tf_df)
    tfidf_df.show(10,true)
    //lda算法训练主题模型
    val lad_in_df=tfidf_df.select("weibo_id","tfidf")
    println("训练主题模型：")
    val ldaModel = new LDA()
      .setK(80)
      .setMaxIter(10)
      .setTopicDistributionCol("topicDistribution")
      .setFeaturesCol("tfidf")
      .fit(lad_in_df)
    //分别使用两种方法进行评估

    // 训练得到的topic
    println("训练的到topic：")
    val topics:DataFrame = ldaModel.describeTopics(5)
    topics.show(10,false)
//    +-----+-----------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
//    |topic|termIndices                        |termWeights                                                                                                                                                                                                      |
//    +-----+-----------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
//    |0    |[9, 14, 7, 2, 5, 4, 0, 11, 12, 17] |[0.13464363481943187, 0.1149293100230011, 0.10341813495504347, 0.07659227587422017, 0.07332789905370332, 0.07063327331012158, 0.06685992151978691, 0.0532400849358462, 0.05164761377712268, 0.04733375399374684] |
//    |1    |[18, 14, 0, 3, 17, 19, 11, 4, 5, 2]|[0.1526936382494602, 0.11801445624662318, 0.11175469528960076, 0.07658606391009669, 0.05835264348176547, 0.05664874773918328, 0.05323783290366223, 0.05185935708737203, 0.0488310422998242, 0.04488182540470127] |
//    |2    |[15, 0, 4, 1, 6, 16, 12, 13, 7, 3] |[0.13770227709866806, 0.12265080274799153, 0.12125824335508548, 0.10749528855161308, 0.0886011904806563, 0.08334669107827784, 0.06104218670124692, 0.04462627064740823, 0.03109798621792386, 0.02949112175696441]|
//    +-----+-----------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    //计算topic_id和对应的词
    println("topic_id,topic_words")
    val topic_words:DataFrame=topics.rdd.map(t=>(t.getAs[Int]("topic"),t.getAs[mutable.WrappedArray[Int]]("termIndices"))).map(t=>{
      (t._1,t._2.toArray.map(vocabulary(_)).reduce(_+","+_))
    }).toDF("topic_id","topic_words")
    topic_words.show()

    println("训练集的toipc分布：")
    println(ldaModel.topicsMatrix)
//    7.9184898732846785  4.9929866651613235  14.245367452909127
//    0.994478020347257   0.9116892436554024  12.485119139583455
//    9.07113180954526    2.0052343677646363  2.1292860507493647
//    3.190173403738846   3.421719283018697   3.425267969013114
//    8.365383128031665   2.3169771769328307  14.083627620739364
//    8.684518511050765   2.1816778473346035  2.137544591830365
//    3.213501443879772   1.085543749559128   10.290650259790588
//    12.24822637748902   1.0612036231510804  3.6118984205106113
//    2.770340157705475   0.8733677664387609  3.0554299002128387
//    15.946388128864042  0.9359159037000092  2.323039796991545
//    4.848268879929585   0.905528876777148   1.1909315808970204
//    6.305437754552989   2.378564847599808   0.8438466280487665
//    6.116834979419336   1.061958813977396   7.089789550542381
//    1.0587089914946513  1.1881646659260312  5.183150938942495
//    13.61154121743022   5.2726608471231335  2.413123407204636
//    1.0916075371828005  1.0694595166717453  15.993531982039654
//    3.914974588973669   1.0553363555394562  9.680362572381235
//    5.6059271854381185  2.6070848300946468  1.0352844906371759
//    0.9842995249419078  6.822060564514518   2.434278604581901
//    2.493797323261399   2.5309580177088975  2.494202569950917

    //通过训练好的主题模型查看某一文档集合的topic的分布
    println("计算topic的分布：")
    val topicdistribution_df:DataFrame = ldaModel.transform(lad_in_df)
    topicdistribution_df.show(10,false)
//    +---------+-------------------------------------------------------------------------+-------------------------------------------------------------+
//    |weibo_id |tfidf                                                                    |topicDistribution                                            |
//    +---------+-------------------------------------------------------------------------+-------------------------------------------------------------+
//    |Fem39mUl7|(20,[0,2,6],[0.9733993280912864,1.983001691780389,2.042820186991299])    |[0.4283287964996816,0.062172902778199875,0.5094983007221185] |
//    |FesD85rPY|(20,[0,12,16],[0.9733993280912864,2.1666144043379636,2.5046382316505955])|[0.055352964051019596,0.05200878423256888,0.8926382517164115]|
//    |Fes7HyvmO|(20,[0],[0.9733993280912864])                                            |[0.18763583232666325,0.21128068924674798,0.6010834784265887] |
//    |FeoEgaeRg|(20,[0],[0.9733993280912864])                                            |[0.18763396247280192,0.21125295462634947,0.6011130829008485] |
//    |FenG0hUjm|(20,[0,12],[0.9733993280912864,2.1666144043379636])                      |[0.09356651929797898,0.08547033339951297,0.8209631473025082] |
//    |Fele9iLFk|(20,[1,11],[2.214242453327218,2.335078926470492])                        |[0.4544758451950655,0.07519353130095631,0.47033062350397825] |
//    |FekBoEqjr|(20,[1,5,9],[2.214242453327218,2.1901449017481576,2.382035909558263])    |[0.6039176393252262,0.04493820808876117,0.35114415258601256] |
//    |FekjVFCu1|(20,[0],[0.9733993280912864])                                            |[0.18762098132402258,0.2110565917204288,0.6013224269555486]  |
//    |FeaXxfcjG|(20,[0],[0.9733993280912864])                                            |[0.1876283052182358,0.21116703644079565,0.6012046583409685]  |
//    |Fedq75YL3|(20,[0],[0.9733993280912864])                                            |[0.18761331970048972,0.21094123727640907,0.6014454430231012] |
//    +---------+-------------------------------------------------------------------------+-------------------------------------------------------------+

    //思考是对主题的权重进行聚类还是对主题的概率分布进行聚类才能得到话题：
    /*******************************************************************************/
//    val kmeans: KMeans = new KMeans().setK(5).setFeaturesCol("termWeights")
//    //topics.select($"".asInstanceOf[])
//    val kmeansModel = kmeans.fit(topics)
//    kmeansModel.summary.predictions.show(false)
//    kmeansModel.clusterCenters.foreach(println(_))
    /*******************************************************************************/

//
//
//
    val kmeansModel: KMeansModel = new KMeans()
      .setK(20)
      .setFeaturesCol("topicDistribution")
    .setPredictionCol("theme_id")
      .fit(topicdistribution_df)

    println("kmeans的训练结果的模型的clusterCenters(theme):")
    kmeansModel.clusterCenters.foreach(println(_))

    //计算theme+topic1+topic2
    println("theme+topic1+topic2")
    val structSchema=StructType(
      StructField("theme_id", IntegerType, true)::
        StructField("topic1_id", IntegerType, false)::
        StructField("topic2_id", IntegerType, false)::Nil)

    val theme_t1_t2_t3:DataFrame=ss.sparkContext.makeRDD(kmeansModel.clusterCenters.zipWithIndex.map(t=>{
      val array=t._1.toArray
      val sorted=array.sortWith(_>_)
      val topic_index_1=array.indexOf(sorted(0))
      val topic_index_2=array.indexOf(sorted(1))
      val topic_index_3=array.indexOf(sorted(2))
      (t._2,topic_index_1,topic_index_2,topic_index_3)
    })).toDF("theme_id","topic1_id","topic2_id","topic3_id")
    theme_t1_t2_t3.show()
    println("通过KMeans计算theme在微博上的分布：")
    val themedistribution_df=kmeansModel.transform(topicdistribution_df)
    themedistribution_df.show(10,false)
    val weibo_theme=themedistribution_df.select("weibo_id","theme_id")
    //计算weibo_id+prediction
    weibo_theme.show()
    //找到每个微博的话题下标
    (weibo_theme,theme_t1_t2_t3,topic_words)
  }
}