import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

object TIW6RDDUtils {

  /**
   * Classe reprise de  https://stackoverflow.com/questions/23995040/write-to-multiple-outputs-by-key-spark-one-spark-job
   * Elle permet de déterminer un nom de fichier en fonction d'une clé pour chaque valeur à écrire
   */
  class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
    override def generateActualKey(key: Any, value: Any): Any =
      NullWritable.get()

    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
      "Source-%s-%s.txt".format(key.asInstanceOf[String], name)
  }

  /**
   * Ecrit le contenu d'un RDD consitué d'éléments qui sont des paires (clé,valeur) dans différent fichiers.
   * Le nom du fichier ou chaque valeur est écrite est donné par la clé.
   *
   * @param rddToWrite le rddAEcrire dans HDFS de la forme : "nom du fichier", "contenu"
   * @param dir        le répertoire qui contiendra les fichiers pour chaque case
   * @param nbTopMax   le nombre approximatif de top par fichier
   */
  def writePairRDDToHadoopUsingKeyAsFileName(rddToWrite: RDD[(String, String)], dir: String, nbTopMax: Int): Unit = {
    rddToWrite
      .partitionBy(new HashPartitioner(nbTopMax))
      .saveAsHadoopFile(dir, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])
  }

  /**
   * Classe reprise de  https://stackoverflow.com/a/48712134
   * Elle permet de créer un seul fichier avec la fonction saveAsSingleTextFile
   */
  implicit class RDDExtensions(val rdd: RDD[String]) extends AnyVal {

    def saveAsSingleTextFile(path: String): Unit =
      saveAsSingleTextFileInternal(path, None)

    def saveAsSingleTextFile(path: String, codec: Class[_ <: CompressionCodec]): Unit =
      saveAsSingleTextFileInternal(path, Some(codec))

    private def saveAsSingleTextFileInternal(
      path: String, codec: Option[Class[_ <: CompressionCodec]]
    ): Unit = {

      // The interface with hdfs:
      val hdfs = FileSystem.get(rdd.sparkContext.hadoopConfiguration)

      // Classic saveAsTextFile in a temporary folder:
      hdfs.delete(new Path(s"$path.tmp"), true) // to make sure it's not there already
      codec match {
        case Some(codec) => rdd.saveAsTextFile(s"$path.tmp", codec)
        case None => rdd.saveAsTextFile(s"$path.tmp")
      }

      // Merge the folder of resulting part-xxxxx into one file:
      hdfs.delete(new Path(path), true) // to make sure it's not there already
      FileUtil.copyMerge(
        hdfs, new Path(s"$path.tmp"),
        hdfs, new Path(path),
        true, rdd.sparkContext.hadoopConfiguration, null
      )

      hdfs.delete(new Path(s"$path.tmp"), true)
    }
  }
}
