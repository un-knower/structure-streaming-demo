package cn.com.gf.whuan

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.influxdb.dto.Point
import play.api.libs.json.Json

/**
  * Created by huanwang on 2017/5/26.
  */
object StructureStreaming {
    def main(args: Array[String]): Unit = {
        if(args.length != 1) {
            println("Usage: StructureStreaming config_file_path")
            System.exit(-1)
        }
        val props = ConfUtil.load(args(0))
        println("config settings: " + props.mkString(", "))

        val spark = SparkSession.builder.appName(props("app.name")).getOrCreate()
        spark.conf.set("spark.streaming.stopGracefullyOnShutdown", true)
        // 读取kafka数据
        val dataSource = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", props("kafka.bootstrap.servers"))
            .option("subscribe", props("subscribe"))
            .load()

        import spark.implicits._
        // 只需要kafka中的value字段，丢弃key字段
        val lines = dataSource.selectExpr("CAST(value AS STRING)").as[Array[Byte]]
        // 解析json，提取需要的字段，转化为DataSet，这里需要实现json解析方法
        val ds = lines.map(s => Extractor.jsonParse(s.toString))
        // 对数据进行窗口划分和滑动，增加过期水印，这里需要按照统计来自动生成
        val windowedCount = ds.withWatermark("ts", props("watermark.time")).groupBy(window($"ts", props("window.size"), props("slide.size")), $"event").count()

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
case class SampleData(event: String, ts: Timestamp) extends Record(ts)

/**
  * json解析方法
  */
object Extractor extends ItemExtractor[SampleData] {
    override def jsonParse(data: String) = {
        val jsObj = Json.parse(data)
        val event = (jsObj \ "event").as[String]
        val timestamp = Timestamp.valueOf((jsObj \ "t").as[String])
        SampleData(event, timestamp)
    }
}

/**
  * 存入influxdb的语句
  * @param props influxdb的配置项
  */
class Destination(props: Map[String, String]) extends InfluxDBWriter(props) {
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