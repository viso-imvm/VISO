package RelationType.Util

import RelationType.Attributes
import RelationType.Relation
import java.io.FileWriter
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.util.Collector

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.scalactic.Bool
import scala.annotation.varargs
import com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.Attribute

class GlobalInfo extends java.io.Serializable {

  var output_result_schema: Array[Array[(Relation, Int, Int)]] = _
  var outputFilePath: String = "out.txt"
  var currentFileLine: Long = 0
  var currentOperation: String = ""
  var currentTuple: Attributes = _
  var currentRelationName: String = ""

  var debugTargetLine: Long = -1L
  var debugTargetValue: String = ""

  def shouldDebug(tupleOrKeys: Attributes): Boolean = {
    (currentFileLine == debugTargetLine) || (debugTargetValue.nonEmpty && tupleOrKeys.values.exists(v => v != null && v.toString.contains(debugTargetValue)))
  }

  def isTargetLine: Boolean = false

  def debugContext(tag: String, extra: String = ""): Unit = {
    println(s"[$tag] line=$currentFileLine op=$currentOperation rel=$currentRelationName tuple=$currentTuple$extra")
  }

  def PrintOutputSchema(): Unit = {
    println("Output Schema:")
    for((out_schema, idx) <- output_result_schema.zipWithIndex)
    {
      val schemaStr = out_schema.map { case (rel, place_id, attr_idx) =>
        s"(${rel.name}, $place_id, $attr_idx)"
      }.mkString(", ")
      println(s"Output $idx: [$schemaStr]")
    }
  }

}
