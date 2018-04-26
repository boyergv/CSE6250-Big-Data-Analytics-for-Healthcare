/**
 * @author Hang Su <hangsu@gatech.edu>.
 */
package edu.gatech.cse8803.ioutils

import java.io.File

import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.SQLContext
import com.databricks.spark.csv.CsvContext
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.rdd.RDD


object CSVUtils {
  def loadCSVAsTable(sqlContext: SQLContext, path: String, tableName: String): SchemaRDD = {
    val data = sqlContext.csvFile(path)
    data.registerTempTable(tableName)
    data
  }

  def loadCSVAsTable(sqlContext: SQLContext, path: String): SchemaRDD = {
    loadCSVAsTable(sqlContext, path, inferTableNameFromPath(path))
  }

  private val pattern = "(\\w+)(\\.csv)?$".r.unanchored
  def inferTableNameFromPath(path: String) = path match {
    case pattern(filename, extension) => filename
    case _ => path
  }

  def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }

  def saveAsSingleLocalFile(rdd: RDD[String], outFile: String) = {

    val file = "/tmp/sparkTempFile"

    FileUtil.fullyDelete(new File(file))
    rdd.saveAsTextFile(file)

    FileUtils.deleteQuietly(new File(outFile.replaceFirst("file:", "")))
    merge(file, outFile)

    FileUtil.fullyDelete(new File(file))
  }
}

