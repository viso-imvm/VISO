package RelationType.Util

import RelationType.Attributes
import RelationType.Relation
import RelationType.Enumerator.fullAttrEnumeratorModified
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

class OutputTool(rel:Relation) {

  def GetStr(operator: String, i:(Int, Attributes)): String =
  {
    operator + " " + i._1+" "+ i._2.values.mkString(", ")
  }

  def outputResult(t : Iterator[(Int, Attributes)], operator : String, outputPath : String = null): Unit = {

    val effectiveOutputPath = if (outputPath != null) outputPath else rel.glob_info.outputFilePath

    val dem = 0

    dem match {
      case 0 => for (i <- t) i
      case 1 => {
        val fw = new FileWriter(effectiveOutputPath, true)
        try {
          for (i <- t) fw.write(GetStr(operator, i) + "\n" )
        }
        finally fw.close()
      }
      case 2 => for (i <- t) println(GetStr(operator, i))
      case 3 => for (i <- t) rel.out.collect(GetStr(operator, i))
      case 4 =>
      case 5 => for (i <- t) rel.out.collect(GetStr(operator, i) + " lat: " + (System.currentTimeMillis()-rel.time.value()).toString)
      case _ => throw new NoSuchMethodException("Output Mode " + rel.deltaEnumMode + " is not supported, please check your code")
    }

  }

  def WitnessTupleTriggerEnum(tuple: Attributes, operator: String = "Insert",ui:UpwardInfo,qs: Array[QueryId]) : Unit =
  {

    val t = new fullAttrEnumeratorModified(rel,tuple, operator,ui,qs)
    if(operator == "Insert")
      outputResult(t, "+")
    else
      outputResult(t, "-")

  }
}
