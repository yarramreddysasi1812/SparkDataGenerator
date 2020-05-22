package in.pramati.mocker.utils

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec

object FileSystemUtils {

  val logger: Logger = Logger.getLogger(getClass.getName)
  val spark: SparkSession = SparkSession.getDefaultSession.get
  val conf = spark.sparkContext.hadoopConfiguration
  val fetchFileSystem: FileSystem = FileSystem.get(conf)
  def readFile(pathUri: String): String = {
    val fs = new Path(pathUri).getFileSystem(conf)
    val path = new Path(pathUri)
    val reader = new BufferedReader(new InputStreamReader(fs.open(path)))

    @tailrec
    def readContent(builder: StringBuilder, reader: BufferedReader): String = {
      val line = reader.readLine()
      if (line != null) {
        builder.append(line)
        readContent(builder, reader)
      } else {
        builder.mkString
      }
    }
    try {
      val builder = new StringBuilder
      readContent(builder, reader)
    } finally {
      reader.close()
    }
  }
}
