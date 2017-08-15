package cn.com.gf.whuan

import org.apache.spark.sql.{ForeachWriter, Row}
import org.influxdb.{InfluxDB, InfluxDBFactory}

/**
  * Created by huanwang on 2017/6/13.
  */
class InfluxDBWriter(props: Map[String, String]) extends ForeachWriter[Row] {
    // influxdb 连接
    var connection: InfluxDB = null

    override def open(partitionId: Long, version: Long): Boolean = {
        connection = InfluxDBFactory.connect(props("influx.url"), props("influx.user"), props("influx.password"))
        true
    }

    override def process(row: Row): Unit = {
    }
    override def close(errorOrNull: Throwable): Unit = {
    }
}
