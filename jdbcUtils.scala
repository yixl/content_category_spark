
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object jdbcUtils {
  /**
   * 连接mysql db
   *
   * @param spark
   * @param table
   * @return
   */

  def connectMysql(spark: SparkSession, db: String, table: String): DataFrame = {
    import java.util._
    val url = "jdbc:mysql://xxx.xxx.x.xxx:3306/" + db + ""
    val props = new Properties()
    props.put("user", "xxx")
    props.put("password", "xxx")
    spark.read.jdbc(url, table, props)
  }

  def saveMysql(spark: SparkSession, db: String, data: DataFrame, table: String): Unit = {
    import java.util._
    val url = "jdbc:mysql://xxx.xxx.x.xxx:3306/" + db + ""
    val props = new Properties()
    props.put("user", "xxx")
    props.put("password", "xxx")
    data.write.mode("append").jdbc(url, table, props)
  }

}
