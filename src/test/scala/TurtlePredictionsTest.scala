import better.files.File
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import java.io.{File => JFile}
import java.text.SimpleDateFormat
import java.util.Date
import scala.reflect.io.Directory

class TurtlePredictionsTest
    extends AnyFunSuite
    with BeforeAndAfter
    with BeforeAndAfterAll {
  var sc: SparkContext = _;

  def checkObservations(
      result: RDD[(String, String)],
      supposedSize: Int
  ): Any = {
    result.cache()
    val groupedResult = result.groupBy(_._1)
    assert(groupedResult.count() == supposedSize)
  }

  test("Vérifie la version case class avec Source-299.csv") {
    TurtlePredictions
        .displayValues(
          "small", 15, 8, 122, 15.2, 0.8, 42
        )
  }

  override def beforeAll: Unit = {
    val myDir = new JFile("save")
    myDir.mkdir()
  }

  before {
    // configuration de Spark
    val conf = new SparkConf()
      .setAppName("TurtlePredictionsTest")
      .setMaster(
        "local"
      )
    sc = new SparkContext(conf)
  }

  after {
    // We want to save all tests
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd_hh-mm-ss")
    val forZip = File("indexed")
    val destination =
      File("save/test-%s.zip".format(dateFormatter.format(new Date())))
    forZip.zipTo(destination)
    val directory = new Directory(new JFile("indexed"))
    directory.deleteRecursively()
    sc.stop()
  }
}
