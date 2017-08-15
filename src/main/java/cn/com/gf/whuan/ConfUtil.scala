package cn.com.gf.whuan

import java.io.FileInputStream
import java.util.Properties

import play.api.libs.json.Json

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
  * Created by huanwang on 2017/6/13.
  */
object ConfUtil {
    /**
      *
      * @param fileName config file path
      * @return configuration
      */
    def load(fileName: String) : Map[String, String] = {
        val config = mutable.Map.empty[String, String]
        val properties = new Properties();
        val in = new FileInputStream(fileName)
        properties.load(in)
        in.close()
        for(k: String <- properties.stringPropertyNames().asScala) {
            config += (k -> properties.getProperty(k))
        }
        config.toMap
    }

}
