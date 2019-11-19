package com.lcy.scala.spark.Util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}
import java.util.Properties
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

/**
  * JDBC 工具类
  * @author Created by CN on 2018/12/5/0005 11:09 .
  */
class DBUtil {

  private var connection: Connection = _

  private var preparedStatement: PreparedStatement = _

  private var resultSet: ResultSet = _

  /**
    * 执行指定的SQL
    * @param sql 需要执行的sql
    */
  def execute(sql: String): Unit = {
    try {
      this.connection = DBUtil.getConnection
      this.preparedStatement = this.connection.prepareStatement(sql)
      this.preparedStatement.execute()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      DBUtil.returnConnection(this.connection)
      this.connection == null
    }
  }

  /**
    * 添加记录并返回主键
    * @param sql
    * @param objects
    * @return
    */
  def insertForGeneratedKeys(sql: String, objects: Array[Any]): ResultSet = {
    try {
      this.connection = DBUtil.getConnection
      this.preparedStatement = this.connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
      if(objects != null)
        this.setPreparedStatement(objects)

      this.preparedStatement.executeUpdate()
      this.resultSet = preparedStatement.getGeneratedKeys
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      DBUtil.returnConnection(this.connection)
      this.connection == null
    }

    this.resultSet
  }

  /**
    * 执行查询
    * @param sql 需要执行的sql
    * @param objects 参数
    * @return
    */
  def executeQuery(sql: String, objects: Array[Any]): ResultSet = {
    try {
      this.connection = DBUtil.getConnection
      this.preparedStatement = this.connection.prepareStatement(sql)
      if(objects != null)
        this.setPreparedStatement(objects)

      this.resultSet = this.preparedStatement.executeQuery()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      DBUtil.returnConnection(this.connection)
      this.connection == null
    }
    this.resultSet
  }

  /**
    * 执行插入/更新操作
    * @param sql
    * @param objects
    * @return 更新的行数，如果-1则出现异常
    */
  def executeInsertOrUpdate(sql: String, objects: Array[Any]): Int = {
    try {
      this.connection = DBUtil.getConnection
      this.preparedStatement = this.connection.prepareStatement(sql)
      if(objects != null)
        this.setPreparedStatement(objects)

      this.preparedStatement.executeUpdate()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      DBUtil.returnConnection(this.connection)
      this.connection == null
    }

    -1
  }

  /**
    * a对于已经初始化完毕的preparedStatement进行参数赋值
    * @param objects
    */
  private def setPreparedStatement(objects: Array[Any]): Unit = {
    var i = 1
    for(obj <- objects) {
      this.preparedStatement.setObject(i, obj)
      i += 1
    }
  }

  /**
    * 存在则更新，不存在则插入
    * @param sqls 0,1,2 查询，更新，插入
    * @param args 0,1,2 查询，更新，插入
    */
  def existsUpdateElseInsert(sqls: List[String], args: List[Array[Any]]): Unit = {
    try {
      if(sqls.length != 3 || args.length != 3)
        return

      // 1. 查询
      val rs = this.executeQuery(sqls.head, args.head)
      if(rs.next()) {
        // 2.1 更新
        if(sqls(1) != null && args(1) != null)
          this.executeInsertOrUpdate(sqls(1), args(1))
      } else {
        // 2.2 插入
        if(sqls(2) != null && args(2) != null)
          this.executeInsertOrUpdate(sqls(2), args(2))
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 释放资源
    */
  def close() : Unit = {
    try {
      if(resultSet != null) resultSet.close()
      if(preparedStatement != null) preparedStatement.close()
      //      if(connection != null) connection.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}



/**
  * JDBC 连接池
  * 配置信息从 config/db.properties 中读取
  * @author Created by CN on 2018/11/9 17:37
  */
object DBUtil {

  private val inputStream = classOf[DBConnectionPool].getClassLoader.getResourceAsStream("DB.properties")
  private val properties = new Properties
  properties.load(inputStream)
  private val driverClass: String = properties.getProperty("jdbc.driver")
  private val url: String = properties.getProperty("jdbc.url")
  private val username: String = properties.getProperty("jdbc.username")
  private val password: String = properties.getProperty("jdbc.password")
  Class.forName(driverClass)
  val poolSize: Int = properties.getProperty("jdbc.max_connection").toInt //连接池总数

  // 连接池 - 同步队列
  private val pool: BlockingQueue[Connection]  = new LinkedBlockingQueue[Connection]()

  /**
    * 初始化连接池
    */
  for(i <- 1 to poolSize) {
    DBUtil.pool.put(DriverManager.getConnection(url, username, password))
  }

  /**
    * 从连接池中获取一个Connection
    * @return
    */
  private def getConnection: Connection = {
    pool.take()
  }


  /**
    * 向连接池归还一个Connection
    * @param conn
    */
  private def returnConnection(conn: Connection): Unit = {
    DBUtil.pool.put(conn)
  }


  /**
    * 启动守护线程释放资源
    */
  def releaseResource() = {
    val thread = new Thread(new CloseRunnable)
    thread.setDaemon(true)
    thread.start()
  }

  /**
    * 关闭连接池连接资源类
    */
  class CloseRunnable extends Runnable{
    override def run(): Unit = {
      while(DBUtil.pool.size > 0) {
        try {
          //          println(s"当前连接池大小: ${DBUtil.pool.size}")
          DBUtil.pool.take().close()
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }
  }

}
