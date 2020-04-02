package bzn.utils

import java.sql.Connection

import com.mchange.v2.c3p0.ComboPooledDataSource

/**
  * Created with IntelliJ IDEA.
  * Author:
  * Description:MySQL DDL 和DML 工具类
  * Date: Created in 2019-11-12
  */
object MySQLPoolManagerBlackGray {
  var mysqlManager: MysqlPool = _

  def getMysqlManager(): MysqlPool = {
    synchronized {
      if (mysqlManager == null) {
        mysqlManager = new MysqlPool()
      }
    }
    mysqlManager
  }

  class MysqlPool() extends Serializable {
    private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)
    try {
      cpds.setJdbcUrl(PropertyUtils.getFileProperties("mysql-user.properties", "mysql.url.cloud_office.dmdb"))
      //      cpds.setJdbcUrl(PropertyUtils.getFileProperties("mysql-user.properties", "mysql.jdbc.url.odsdb"))
      cpds.setDriverClass(PropertyUtils.getFileProperties("mysql-user.properties", "mysql.pool.jdbc.driverClass"))
      cpds.setUser(PropertyUtils.getFileProperties("mysql-user.properties", "mysql.jdbc.username.cloud_office"))
      cpds.setPassword(PropertyUtils.getFileProperties("mysql-user.properties", "mysql.jdbc.password.cloud_office"))
      cpds.setMinPoolSize(PropertyUtils.getFileProperties("mysql-user.properties", "mysql.pool.jdbc.minPoolSize").toInt)
      cpds.setMaxPoolSize(PropertyUtils.getFileProperties("mysql-user.properties", "mysql.pool.jdbc.maxPoolSize").toInt)
      cpds.setAcquireIncrement(PropertyUtils.getFileProperties("mysql-user.properties", "mysql.pool.jdbc.acquireIncrement").toInt)
      cpds.setMaxStatements(PropertyUtils.getFileProperties("mysql-user.properties", "mysql.pool.jdbc.maxStatements").toInt)
    } catch {
      case e: Exception => e.printStackTrace()
    }

    def getConnection: Connection = {
      try {
        cpds.getConnection()
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
          null
      }
    }

    def close(): Unit = {
      try {
        cpds.close()
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
      }
    }
  }

}
