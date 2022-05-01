package corporate

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import scala.collection.immutable.Stream
import org.json4s.JValue

import java.io.{BufferedReader, InputStreamReader}
import scala.util.matching.Regex

abstract class DataMart {

  val FilePath: Regex = """(\w+://)+((?:\d+\.){3}\d+:\d+)?([/\w]+/)+(\w+.json)""".r
  val conf: JValue
  val path: String
  def csv: String = path + (conf \ "csv").values.toString
  def pqt: String = path + (conf \ "pqt").values.toString

  def readText(url: String, path: String, encoding: String = "UTF-8"): String = {
    val conf = new Configuration()
    val fs = url match {
      case u: String if u.startsWith("file://") || u.trim.isEmpty => FileSystem.getLocal(conf).getRawFileSystem
      case u: String => FileSystem.get(new URI(u), conf)
      case _ => throw new IllegalArgumentException("unknown schema")
    }
    val absPath: Path = new Path(path)
    val stream = fs.open(absPath)
    val br = new BufferedReader(new InputStreamReader(stream, encoding))
    try {
      val text: String = Stream.continually(br.readLine).takeWhile(_ != null).mkString
      text
    }
    finally {
      stream.close()
      br.close()
    }
  }
}

/*
  val hdfs = "hdfs://172.18.0.2:8020/tmp/spm/resources/way.json"
  val local = "file://src/main/resources/way.json"
*/
