package Scala.com.zj.utils

import java.io.IOException
import java.util
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, NamespaceDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Durability, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm


/**
  * Created by BigData on 2017/7/6.
  */
class HBaseHelper {
  val conn=HBaseDBUtil.getConnection()
  val admin=conn.getAdmin()
  def executeQuery(cf:String): Unit ={

  }

  def createNamespace(namespace: String): Unit = {
    try {
      val nd = NamespaceDescriptor.create(namespace).build
      admin.createNamespace(nd)
    } catch {
      case e: Exception =>
        System.err.println("Error: " + e.getMessage)
    }
  }

  def dropNamespace(namespace: String, force: Boolean): Unit = {
    try {
      if (force) {
        val tableNames = admin.listTableNamesByNamespace(namespace)
        for (name <- tableNames) {
          admin.disableTable(name)
          admin.deleteTable(name)
        }
      }
    }catch {
      case e: Exception =>
      // ignore
    }
    try{
      admin.deleteNamespace(namespace)
    }catch{
      case e: IOException =>
        System.err.println("Error: " + e.getMessage)
    }
  }

  @throws[IOException]
  def existsTable(table: String): Boolean = existsTable(TableName.valueOf(table))

  @throws[IOException]
  def existsTable(table: TableName): Boolean = admin.tableExists(table)


  @throws[IOException]
  def createTable(table: TableName, maxVersions: Int, splitKeys: Array[Array[Byte]], colfams: String*): Unit = {
    val desc = new HTableDescriptor(table)
    desc.setDurability(Durability.SKIP_WAL)
    for (cf <- colfams) {
      val coldef = new HColumnDescriptor(cf)
      coldef.setCompressionType(Algorithm.SNAPPY)
      coldef.setMaxVersions(maxVersions)
      desc.addFamily(coldef)
    }
    if (splitKeys != null) admin.createTable(desc, splitKeys)
    else admin.createTable(desc)
  }

  @throws[IOException]
  def disableTable(table: String): Unit = {
    disableTable(TableName.valueOf(table))
  }

  @throws[IOException]
  def disableTable(table: TableName): Unit = {
    admin.disableTable(table)
  }

  @throws[IOException]
  def dropTable(table: String): Unit = {
    dropTable(TableName.valueOf(table))
  }

  @throws[IOException]
  def dropTable(table: TableName): Unit = {
    if (existsTable(table)) {
      if (admin.isTableEnabled(table)) disableTable(table)
      admin.deleteTable(table)
    }
  }

  @throws[IOException]
  def splitTable(tableName: String, splitPoint: Array[Byte]): Unit = {
    val table = TableName.valueOf(tableName)
    admin.split(table, splitPoint)
  }

  @throws[IOException]
  def splitRegion(regionName: String, splitPoint: Array[Byte]): Unit = {
    admin.splitRegion(Bytes.toBytes(regionName), splitPoint)
  }

  @throws[IOException]
  def mergerRegions(regionNameA: String, regionNameB: String): Unit = {
    admin.mergeRegions(Bytes.toBytes(regionNameA), Bytes.toBytes(regionNameB), true)
  }

  def padNum(num: Int, pad: Int): String = {
    var res = Integer.toString(num)
    if (pad > 0) while ( {
      res.length < pad
    }) res = "0" + res
    res
  }

  @throws[IOException]
  def put(table: String, row: String, fam: String, quals: Array[String], values: Array[String]): Unit = {
    val tbl = conn.getTable(TableName.valueOf(table))
    val put = new Put((Bytes.toBytes(row)))
    for(i<-0 until quals.length){
      put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(quals(i)), Bytes.toBytes(values(i)))
    }
    tbl.put(put)
    tbl.close
  }

  /*
  * @param rows:行名（可以插入多个行）
  * @param fams:每行可能有多个cf
  * quals：每个cf的多个col
  * values：每行的数据，每个列族的数据，每个col的数据
  * */
  @throws[IOException]
  def puts(table: String, rows: Array[String], fams:Array[String], quals: Array[Array[String]], values: Array[Array[Array[String]]]): Unit = {
    val tbl = conn.getTable(TableName.valueOf(table))
    val putList=new util.ArrayList[Put]()
    for(i<-0 until rows.length){
      val put = new Put((Bytes.toBytes(rows(i))))
      for(j<-0 until fams.length){
        for(k<-0 until quals(j).length){
          put.addColumn(Bytes.toBytes(fams(j)), Bytes.toBytes(quals(j)(k)),Bytes.toBytes(values(i)(j)(k)))
        }
      }
      putList.add(put)
    }
    tbl.put(putList)
    tbl.close
  }
  @throws[IOException]
  def close(): Unit = {
    conn.close
  }
}
