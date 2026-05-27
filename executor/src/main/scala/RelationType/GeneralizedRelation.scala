package RelationType

import RelationType.Util.GlobalInfo
import java.io.FileWriter
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.util.Collector
import Util.TupleConnection

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.scalactic.Bool
import scala.annotation.varargs
import com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.Attribute

abstract class GeneralizedRelation(name : String, override val deltaEnumMode: Int, numChild : Int = 0, override val output_attr: Array[Int] = Array(), override val glob_info: GlobalInfo )
  extends Relation(name, deltaEnumMode, numChild,output_attr, glob_info) {

  override def insert(tuple : Attributes) : Unit = {
    throw new NoSuchMethodError("No insertion is allowed in the Generalized relation")
  }

  override def delete(tuple : Attributes) : Unit = {
    throw new NoSuchMethodError("No Deletion is allowed in the Generalized relation")
  }

  def testExists(tuple : Attributes) : Boolean = {
    connection(0).k2onhold.contains(tuple) || connection(0).k2alive.contains(tuple)

  }

  override protected def insertKeyMap(tuple: Attributes): Unit = {
    for (i <- connection) {
      val keys = tuple.projection(i.parent_jk)
      update_index_tool.addElementToIndex(keys, tuple, i.k2onhold)
    }
  }

  override def updateAlive(keys : Attributes, index : TupleConnection) : Unit = {
    if (glob_info.shouldDebug(keys)) {
      glob_info.debugContext("updateAlive[Gen]", s" rel=$name keys=$keys testExists=${testExists(keys)} k2onhold_contains=${index.k2onhold.contains(keys)} k2alive_contains=${index.k2alive.contains(keys)}")
    }
    if (testExists(keys) == false) {
      insertKeyMap(keys)

      for(ui<-upward_infos)
        update_index_tool.onholdAdd(keys, 0,ui)
    }

    if (index.k2onhold.contains(keys)) {
      val temp = index.k2onhold.get(keys).toArray
      for (i <- temp) {
        var already_changed: Boolean = false

        for(ui<-upward_infos)
        {
          val jk = i.projection(ui.joinkey)
          val tempEle = ui.onhold.get(jk)

          var tempCnt = tempEle.getOrElse(i, throw new Exception("Element not found in the onhold state"))
          tempCnt = tempCnt + 1

          if (tempCnt == numChild) {
            if(!already_changed)
              update_index_tool.changeToAliveConnection(i)
            already_changed = true
            update_index_tool.onholdDelete(i,ui)
            update_index_tool.aliveAdd(i,ui)
          } else {
            tempEle.put(i, tempCnt)
          }
        }

      }
    }
  }

  override def updateOnHold(keys : Attributes, index: TupleConnection) : Unit = {
    if (glob_info.shouldDebug(keys)) {
      glob_info.debugContext("updateOnHold[Gen]", s" rel=$name keys=$keys k2onhold_contains=${index.k2onhold.contains(keys)} k2alive_contains=${index.k2alive.contains(keys)}")
    }
    if (index.k2onhold.contains(keys)) {
      val temp = index.k2onhold.get(keys)
      for (i <- temp) {
        var already_deleted: Boolean = false
        for(ui<-upward_infos)
        {
          val jk = i.projection(ui.joinkey)
          val tempEle = ui.onhold.get(jk)
          var tempCnt = tempEle.getOrElse(i, throw new Exception("Element not found in the onhold state"))
          tempCnt = tempCnt - 1
          if (tempCnt == 0) {
            update_index_tool.onholdDelete(keys,ui)
            if(!already_deleted)
              update_index_tool.deleteKeyMap(keys)
            already_deleted = true
          }
          tempEle.put(i, tempCnt)
        }

      }
    }
    if (index.k2alive.contains(keys)) {
      val temp = index.k2alive.get(keys).toArray
      for (i <- temp) {
        var already_changed: Boolean = false
        for(ui<-upward_infos)
        {
          update_index_tool.aliveDelete(i,ui)
          update_index_tool.onholdAdd(i, numChild-1,ui)
          if(!already_changed)
            update_index_tool.changeToOnHoldConnection(i)
          already_changed = true
          if (numChild == 0) {
            throw new Exception("Generalized relation cannot be a leaf node.")
          }
        }
      }
    }

  }

  override def toString: String = {
    "Relation " + name
  }

}
