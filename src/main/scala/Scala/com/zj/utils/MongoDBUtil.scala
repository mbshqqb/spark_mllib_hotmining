package Scala.com.zj.utils

import com.mongodb.casbah.{MongoClient, MongoDB}
import com.mongodb.{MongoCredential, ServerAddress}

/**
  * Created by BigData on 2017/7/27.
  */
object MongoDBUtil {

  val server = new ServerAddress("172.17.11.171", 27017)
  val credentials:MongoCredential=MongoCredential.createScramSha1Credential("root","admin","root".toArray)
  val mongoClient: MongoClient = MongoClient(server, List(credentials))
  val dbName="zj"
  def getClient():MongoClient={
      mongoClient
  }
  def getDB(): MongoDB ={
    mongoClient.getDB(dbName)
  }
  def colseCollection()={
    mongoClient.close()
  }

}
