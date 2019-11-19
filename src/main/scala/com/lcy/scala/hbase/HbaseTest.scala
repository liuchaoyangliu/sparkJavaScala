package com.lcy.scala.hbase

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Delete, Get, Put, Result, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes

object HbaseTest {

    private val configuration: Configuration = HBaseConfiguration.create()
    private val connection: Connection = ConnectionFactory.createConnection(configuration)
    private val admin: Admin = connection.getAdmin

    def main(args: Array[String]): Unit = {

    }

    def isTableExist(tableName: String): Boolean = {
        admin.tableExists(TableName.valueOf(tableName))
    }


    //创建表
    def createTable(tableName: String, columnFamilys: Array[String]): Unit = {
        val tName: TableName = TableName.valueOf(tableName)
        //判断表是否存在
        if (!admin.tableExists(tName)) {
            //创建表格式
            val descriptor: HTableDescriptor = new HTableDescriptor(tName)
            //列族
            for (columnFamily: String <- columnFamilys) {
                descriptor.addFamily(new HColumnDescriptor(columnFamily))
            }
            //创建表
            admin.createTable(descriptor)
            println("创建表成功！")
        } else {
            println(s"表 $tableName 已经存在！")
        }
    }

    //删除表
    def deleteTable(tableName: String): Unit = {
        val tName :TableName = TableName.valueOf(tableName)
        if (admin.tableExists(tName)) {
            //先使该表变为disable
            admin.disableTable(tName)
            //删除表
            admin.deleteTable(tName)
            println(s"表 $tableName 创建成功！")
        }
    }

    //向表中插入一条数据
    def addRowData(tableName: String,
                   rowKey: String,
                   columnFamily: String,
                   column: String,
                   value: String): Unit = {
        val tName: TableName = TableName.valueOf(tableName)
        //判断表是否存在
        if (admin.tableExists(tName)) {
            //获取表
            val table: Table = connection.getTable(tName)
            //封装数据
            val put : Put = new Put(rowKey.getBytes())
            put.addColumn(columnFamily.getBytes(), column.getBytes(), value.getBytes())
            //将数据插入到表中
            table.put(put)
            table.close()
            println(s"向 $tableName 表中插入数据成功！")
        } else {
            println(s"表 $tableName 不存在")
        }
    }

    //删除一行或多行数据
    def deleteMultiRow(tableName: String, rows: String*) {
        val tName: TableName = TableName.valueOf(tableName)
        if (admin.tableExists(tName)) {
            //获取表对象
            val table: Table = connection.getTable(tName)
            val deleteList = new util.ArrayList[Delete]
            for (row <- rows) {
                val delete: Delete = new Delete(row.getBytes())
                deleteList.add(delete)
            }
            //删除数据
            table.delete(deleteList)
            table.close()
            println(s"删除 $tableName 表中数据成功！")
        } else {
            println(s"表 $tableName 不存在！")
        }
    }

    //获取一行数据
    def getRow(tableName: String, rowKey: String): Result = {
        val tName = TableName.valueOf(tableName)
        if (admin.tableExists(tName)) {
            val table: Table = connection.getTable(tName)
            val get: Get = new Get(rowKey.getBytes())
            val result: Result = table.get(get)
            for (cell: Cell <- result.rawCells()) {
                Bytes.toString(result.getRow)
                println("行键 " + Bytes.toString(result.getRow))
                println("列族：" + Bytes.toString(CellUtil.cloneFamily(cell)))
                println("列：" + Bytes.toString(CellUtil.cloneQualifier(cell)))
                println("值：" + Bytes.toString(CellUtil.cloneValue(cell)))
                println("时间戳：" + cell.getTimestamp)
            }
            table.close()
            result
        } else {
            println(s"表 $tableName 不存在！")
            null
        }
    }

    //获取一张表中所有数据
    def getAllRow(tableName: String): ResultScanner = {
        val tName = TableName.valueOf(tableName)
        if (admin.tableExists(tName)) {
            val table: Table = connection.getTable(tName)
            val scan: Scan = new Scan()
            val resultScanner: ResultScanner = table.getScanner(scan)
            val result  = resultScanner.iterator()
            while (result.hasNext) {
                val cells = result.next().rawCells()
                for (cell: Cell <- cells) {
                    println("行键：" + Bytes.toString(CellUtil.cloneRow(cell)))
                    println("列族：" + Bytes.toString(CellUtil.cloneFamily(cell)))
                    println("列：" + Bytes.toString(CellUtil.cloneQualifier(cell)))
                    println("值：" + Bytes.toString(CellUtil.cloneValue(cell)))
                }
            }
            table.close()
            resultScanner
        } else {
            print(s"表 $tableName 不存在")
            null
        }
    }

    //获取指定列族： 列的数据
    def getRowQualifier(tableName: String,
                        rowKey: String,
                        family: String,
                        qualifier: String): Result ={
        val tName: TableName = TableName.valueOf(tableName)
        if(admin.tableExists(tName)){
            val table: Table = connection.getTable(tName)
            val get:Get = new Get(rowKey.getBytes())
            get.addColumn(family.getBytes(), family.getBytes())
            val result: Result = table.get(get)
            for(cell: Cell <- result.rawCells()){
                println("行键：" + Bytes.toString(CellUtil.cloneRow(cell)))
                println("列族：" + Bytes.toString(CellUtil.cloneFamily(cell)))
                println("列：" + Bytes.toString(CellUtil.cloneQualifier(cell)))
                println ("值：" + Bytes.toString(CellUtil.cloneValue(cell)))
            }
            table.close()
            result
        }else{
            println(s"表 $tableName 不存在！")
            null
        }
    }

    def close(): Unit ={
        admin.close()
        connection.close()
    }

}
