package cn.com.gf.whuan

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.influxdb.dto.Point

/**
  * Created by huanwang on 2017/5/26.
  */
object StructureStreamingCSV {
    def main(args: Array[String]): Unit = {
        if(args.length != 1) {
            println("Usage: StructureStreaming config_file_path")
            System.exit(-1)
        }
        val props = ConfUtil.load(args(0))
        println("config settings: " + props.mkString(", "))

        val spark = SparkSession.builder.appName(props("app.name")).getOrCreate()
        spark.conf.set("spark.streaming.stopGracefullyOnShutdown", true)
        import spark.implicits._
        // 读取kafka数据
        val dataSource = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", props("kafka.bootstrap.servers"))
            .option("subscribe", props("subscribe"))
            .load()

        // 只需要kafka中的value字段，丢弃key字段
        val lines = dataSource.selectExpr("CAST(value AS STRING)").as[String]
        // 解析json，提取需要的字段，转化为DataSet，这里需要实现json解析方法
        val ds = lines.map(Extractor.jsonParse)
        // 对数据进行窗口划分和滑动，增加过期水印，这里需要按照统计来自动生成
        val windowedCount = ds.withWatermark("ts", props("watermark.time"))
          .groupBy(window($"ts", props("window.size"), props("slide.size")), $"event")
          .agg(count("*"))

        // 输出模式为追加，写入到influxdb
        val query = windowedCount.writeStream.foreach(new Destination(props)).outputMode("append").start()

        // 等待查询终止
        query.awaitTermination()
    }
}

/**
  * 从json结构中提取后的数据类型
  * @param event
  * @param ts
  */
case class SampleData1(event: String, ts: Timestamp) extends Record(ts)

/**
  * json解析方法
  */
object Extractor1 extends ItemExtractor[SampleData1] {
    import java.time.LocalDateTime
    import java.time.format.DateTimeFormatter
    override def jsonParse(data: String) = {
        val array = data.split(",")
        val event = array(2)
        val timestamp = Timestamp.valueOf(array(0))
//        val timestamp = Timestamp.valueOf(LocalDateTime.parse("30/07/2015:05:00:50", DateTimeFormatter.ofPattern("dd/MM/yyyy:HH:mm:ss")))
//        val timestamp = new Timestamp("1438203650000".toLong)
        SampleData1(event, timestamp)
    }
}

/**
  * 存入influxdb的语句
  * @param props influxdb的配置项
  */
class Destination1(props: Map[String, String]) extends InfluxDBWriter(props) {
    override def process(row: Row): Unit = {
        val ts = row.getStruct(0).getTimestamp(0).getTime
        val event = row.getString(1)
        val cnt = row.getLong(2)
        val point = Point.measurement(props("influx.measurement")).tag("event", event).addField("cnt", cnt).time(ts, TimeUnit.MILLISECONDS).build()
        connection.write(props("influx.db.name"), props("influx.db.retention"), point)
    }

    override def close(errorOrNull: Throwable): Unit = {
    }
}



