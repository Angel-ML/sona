package org.apache.spark.angel.graph.utils

import java.io.IOException

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.model.output.format.{MatrixFilesMeta, ModelFilesConstent}
import com.tencent.angel.ps.storage.partitioner.Partitioner
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

object MatrixMetaUtils {
  def readMatrixContext(matPath: Path, fs: FileSystem): MatrixContext = {
    var input: FSDataInputStream= null
    var matrixFilesMeta: MatrixFilesMeta = null
    try {
      val metaFilePath = new Path(matPath.getName, ModelFilesConstent.modelMetaFileName)
      if (!fs.exists(metaFilePath)) {
        throw new IOException(s"Can not find meta file for matrix $metaFilePath")
      }
      fs.setVerifyChecksum(false)
      input = fs.open(metaFilePath)
      matrixFilesMeta = new MatrixFilesMeta()
      matrixFilesMeta.read(input)
    } catch {
      case e: IOException =>
        e.printStackTrace()
        throw e
      case e: Exception =>
        e.printStackTrace()
        throw e
      case ae: AssertionError =>
        ae.printStackTrace()
        throw ae
    } finally {
      if (input != null) {
        input.close()
      }
    }

    val mc = new MatrixContext
    mc.setName(matrixFilesMeta.getMatrixName)
    mc.setRowNum(matrixFilesMeta.getRow)
    mc.setColNum(matrixFilesMeta.getCol)
    mc.setMaxRowNumInBlock(matrixFilesMeta.getBlockRow)
    mc.setMaxColNumInBlock(matrixFilesMeta.getBlockCol)
    mc.setRowType(RowType.valueOf(matrixFilesMeta.getRowType))
    mc.getAttributes.putAll(matrixFilesMeta.getOptions)
    if (mc.getAttributes.containsKey(AngelConf.Angel_PS_PARTITION_CLASS)) {
      val partitionClassName = mc.getAttributes.get(AngelConf.Angel_PS_PARTITION_CLASS)
      mc.setPartitionerClass(Class.forName(partitionClassName).asInstanceOf[Class[Partitioner]])
      mc.getAttributes.remove(AngelConf.Angel_PS_PARTITION_CLASS)
    }

    mc
  }
}
