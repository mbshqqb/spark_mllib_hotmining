package Scala.com.zj.utils

import java.io.InputStream
import java.util.Properties

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
/**
  * Created by BigData on 2017/7/6.
  */
object HBaseDBUtil {
  val is: InputStream = this.getClass.getClassLoader.getResourceAsStream("hbasedb.properties")
  val prop=new Properties()
  prop.load(is)
  val rootdir = prop.getProperty("hbase.rootdir")
  val quorum = prop.getProperty("hbase.zookeeper.quorum")
  val is_version_skip = prop.getProperty("hbase.defaults.for.version.skip")

  val hbaseConf = HBaseConfiguration.create()
  hbaseConf.set("hbase.rootdir", "hdfs://billwang129:9000/hbase")
  hbaseConf.set("hbase.zookeeper.quorum", "172.17.11.171:2181,172.17.11.169:2181,172.17.11.173:2181")
  hbaseConf.set("hbase.defaults.for.version.skip", "true")
  def getConnection(): Connection ={
    ConnectionFactory.createConnection(hbaseConf)
  }
}
