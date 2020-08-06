

import com.bru.utils.jdbcUtils._
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

object processBloggerCategory {
  /**
   * create 2020-04-01
   * author bruce
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("category demo").enableHiveSupport().getOrCreate()
    process_category_userPortrait_(spark, content, "1-10")
  }

  // 根据自定义标签库，给文本打标签
  def process_category_userPortrait_(spark: SparkSession, content: DataFrame, category_id: String): DataFrame = {
    import spark.implicits._
    import scala.collection.mutable

    var cate_id: List[Int] = List()
    if (category_id.contains("-")) {
      cate_id = (category_id.split("-")(0).toInt until category_id.split("-")(1).toInt + 1).toList
    } else {
      cate_id = category_id.split(",").map(x => x.toInt).toList
    }
    for (i <- cate_id) {
      // 读取标签库
      val data = mysqlUtils("databases","tablename").filter("id = '" + i + "'")

      // 映射到二级标签
      val tag_id = data.rdd.map { line =>
        val tags = line.getString(1).split("\\|").filter(x => x.length > 1).distinct.toList
        val id = line.getInt(0)
        (tags, id)
      }.toDF("tags", "id").withColumn("tag", explode(col("tags"))).select("tag", "id").rdd.map { line =>
        val tag = line.getString(0)
        val id = line.getInt(1)
        (tag, id)
      }.groupByKey()
        .mapValues(x => x.toList.distinct.mkString(","))
        .toDF("tag", "id")
        .selectExpr("concat(tag,'|',id) as tag_id")
        .rdd.map { line =>
        line.getString(0)
      }.collect().toList

      // 处理标签数据
      val tags = data.rdd.map(x => x.getString(1))
        .filter(x => x.length > 0)
        .flatMap(x => x.split("\\|"))
        .distinct()
        .collect()
        .toList

      // 处理文本数据
      val res = content.rdd.map { line =>
        val uid = line.getString(0)
        val content = line.getString(1)
        val str: mutable.StringBuilder = new StringBuilder()
        for (i <- tags) {
          if (content.contains(i.toString.trim)) {
            str.append(i + "|")
          }
        }
        (uid, content, str.toString())
      }.map(x => (x._1, x._3))
        .groupByKey()
        .mapValues(x => x.toList.mkString("").trim)
        .filter(x => x._2.length > 0)
        .map { line =>
          val uid = line._1
          val tags = line._2.split("\\|").toList
          val str: StringBuilder = new mutable.StringBuilder()
          val str0: StringBuilder = new mutable.StringBuilder()
          for (j <- tags) {
            for (i <- tag_id) {
              if (i.split("\\|")(0).equals(j)) {
                str.append(i.split("\\|")(1) + ",")
                str0.append(i.split("\\|")(0) + ",")
              }
            }
          }
          (uid, tags.mkString("|"), str.toString(), str0.toString())
        }.map(
        x => (x._1, x._2, x._3.split(",")
          .map(x => (x, 1))
          .groupBy(x => x._1)
          .mapValues(x => x.length)
          .toList
          .sortBy(x => x._2)
          .reverse
          .map(x => (x._1 + "|" + x._2))
          .slice(0, 5)
          .mkString("|")
          , x._4.split(",")
          .map(x => (x, 1))
          .groupBy(x => x._1)
          .mapValues(x => x.length)
          .toList
          .sortBy(x => x._2)
          .reverse
          .map(x => (x._1 + "|" + x._2))
          .mkString(" ")
        )
      ).map(x =>
        (x._1,
          x._3.split("\\|")(0),
          x._3.split("\\|")(1),
          x._4.split(" ").toList.size,
          x._4))
        .toDF()

      res.write.mode("append").saveAsTable("ads_content_tags.table_tags")
    }
    // 对结果数据进行聚合
    process_userportrait_cate(spark,res)
  }

  def process_userportrait_cate(spark: SparkSession,res:DataFrame): Unit = {
    import spark.implicits._
    // 标签聚合映射
    val match_blogger = res
    val tags = mysqlUtils("databases","tablename").selectExpr("id", "pid")
    val cate = match_blogger.join(tags, Seq("id"))
    // 更新分类标签，排序
    cate.rdd.map { line =>
      (line.getString(0), (line.getInt(2), line.getInt(3)))
    }.groupByKey().mapValues(x => (x.toList.sortBy(_._2).reverse.map(_._1).slice(0, 3).mkString(" "),
      x.toList.sortBy(_._2).reverse.map(_._2).slice(0, 3).mkString(" "),
      (x.toList.sortBy(_._2).reverse.map(_._2).slice(0, 3).sum * 1.0)./(x.toList.sortBy(_._2).reverse.map(_._2).sum * 1.0)))
      .map(x => (x._1, x._2._1, x._2._2, x._2._3))
      .toDF()
    。show()
  }

}
