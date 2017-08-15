package cn.com.gf.whuan

/**
  * Created by huanwang on 2017/6/13.
  */
abstract class ItemExtractor[T] {
    def jsonParse(data: String): T
}
