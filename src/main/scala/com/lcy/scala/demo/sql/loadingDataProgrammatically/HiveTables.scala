package com.lcy.scala.demo.sql.loadingDataProgrammatically

import java.io.File

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

case class Record(key: Int, value: String)

object HiveTables {

  def main(args: Array[String]): Unit = {

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

    spark.sql("SELECT * FROM src").show()
    // +---+-------+
    // |key|  value|
    // +---+-------+
    // |238|val_238|
    // | 86| val_86|
    // |311|val_311|
    // ...

    spark.sql("SELECT COUNT(*) FROM src").show()
    // +--------+
    // |count(1)|
    // +--------+
    // |    500 |
    // +--------+

    val sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }

    stringsDS.show()
    // +--------------------+
    // |               value|
    // +--------------------+
    // |Key: 0, Value: val_0|
    // |Key: 0, Value: val_0|
    // |Key: 0, Value: val_0|
    // ...

    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")

    spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()
    // +---+------+---+------+
    // |key| value|key| value|
    // +---+------+---+------+
    // |  2| val_2|  2| val_2|
    // |  4| val_4|  4| val_4|
    // |  5| val_5|  5| val_5|
    // ...

    spark.sql("CREATE TABLE hive_records(key int, value string) STORED AS PARQUET")
    val df = spark.table("src")
    df.write.mode(SaveMode.Overwrite).saveAsTable("hive_records")
    spark.sql("SELECT * FROM hive_records").show()
    // +---+-------+
    // |key|  value|
    // +---+-------+
    // |238|val_238|
    // | 86| val_86|
    // |311|val_311|
    // ...

    val dataDir = "/tmp/parquet_data"
    spark.range(10).write.parquet(dataDir)
    spark.sql(s"CREATE EXTERNAL TABLE hive_ints(key int) STORED AS PARQUET LOCATION '$dataDir'")
    spark.sql("SELECT * FROM hive_ints").show()
    // +---+
    // |key|
    // +---+
    // |  0|
    // |  1|
    // |  2|
    // ...

    // 打开Hive动态分区标志
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    // 使用DataFrame API创建一个Hive分区表
    df.write.partitionBy("key").format("hive").saveAsTable("hive_part_tbl")
    // 分区列' key '将移动到模式的末尾。
    spark.sql("SELECT * FROM hive_part_tbl").show()
    // +-------+---+
    // |  value|key|
    // +-------+---+
    // |val_238|238|
    // | val_86| 86|
    // |val_311|311|
    // ...

    spark.stop()

  }

}
