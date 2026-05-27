package RelationType

import Util.{UpwardInfo,TupleConnection, QueryId, GlobalInfo,UpdateIndexTool,UtilTool,OutputTool,InitTool}
import Enumerator.{fullAttrEnumeratorModified, joinKeyEnumeratorModified, deltaEnumeratorModified}
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
import RelationType.Util.InitTool

abstract class Relation(val name : String,  val deltaEnumMode: Int, var numChild : Int = 0, val output_attr: Array[Int] = Array(), val glob_info : GlobalInfo = new GlobalInfo()) {

  var upward_infos: ArrayBuffer[UpwardInfo] = new ArrayBuffer()
  var output_project_info : mutable.HashMap[QueryId, Array[Int]] = mutable.HashMap.empty
  var time : ValueState[Long] = _

  var connection : ArrayBuffer[TupleConnection] = new ArrayBuffer()

  var conn_cnt = 0
  var up_conn_cnt = 0
  var runtime : RuntimeContext = _

  var out : Collector[String] = null

  val init_tool: InitTool  = new InitTool(this)
  val update_index_tool: UpdateIndexTool = new UpdateIndexTool(this)
  val util_tool: UtilTool = new UtilTool(this)
  val output_tool: OutputTool = new OutputTool(this)

  protected def insertKeyMap(tuple : Attributes) : Unit = {
    val c : Int = testAlive(tuple)
    for (i <- connection) {
      val keys = tuple.projection(i.parent_jk)
      if (c == numChild) {
        update_index_tool.addElementToIndex(keys, tuple, i.k2alive)
      } else {
        update_index_tool.addElementToIndex(keys, tuple, i.k2onhold)
      }
    }
  }

  def testAlive(tuple: Attributes) : Int = {

    if (numChild == 0) 0
    else {
      var cnt = 0
      for (i <- connection) {
        if (i.ui.keyCount.get(tuple.projection(i.parent_jk)) > 0) cnt = cnt + 1
      }

      cnt
    }
  }

  def updateAlive(keys : Attributes, index : TupleConnection) : Unit = {
    if (glob_info.shouldDebug(keys)) {
      glob_info.debugContext("updateAlive", s" rel=$name keys=$keys k2onhold_contains=${index.k2onhold.contains(keys)} k2alive_contains=${index.k2alive.contains(keys)}")
    }

    if (index.k2onhold.contains(keys)) {
      val temp = index.k2onhold.get(keys).toArray
      for (i <- temp) {
        var already_changed: Boolean = false
        for(ui <- upward_infos)
        {
          val jk = i.projection(ui.joinkey)
          val tempEle = ui.onhold.get(jk)
          var tempCnt = tempEle.getOrElse(i, throw new Exception("Element not found in the onhold state. tuple: "+i.toString()+", ui jk: "+ui.joinkey.mkString(",")))
          tempCnt = tempCnt + 1

          if (tempCnt == numChild) {
            if(already_changed == false)
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

  def updateOnHold(keys : Attributes, index: TupleConnection) : Unit = {
    if (glob_info.shouldDebug(keys)) {
      glob_info.debugContext("updateOnHold", s" rel=$name keys=$keys k2onhold_contains=${index.k2onhold.contains(keys)} k2alive_contains=${index.k2alive.contains(keys)}")
    }
    if (index.k2onhold.contains(keys)) {
      val temp = index.k2onhold.get(keys)
      for (i <- temp) {
        for(ui<- upward_infos)
        {
          val jk = i.projection(ui.joinkey)
          val tempEle = ui.onhold.get(jk)
          var tempCnt = tempEle.getOrElse(i, throw new Exception("Element not found in the onhold state"))
          tempCnt = tempCnt - 1
          tempEle.put(i, tempCnt)
        }

      }
    }
    if (index.k2alive.contains(keys)) {
      val temp = index.k2alive.get(keys).toArray
      for (i <- temp) {
        var already_changed: Boolean = false
        for(ui<- upward_infos)
        {
          update_index_tool.aliveDelete(i,ui)
          if (numChild > 0) {
            update_index_tool.onholdAdd(i, numChild - 1,ui)
            if(already_changed == false)
              update_index_tool.changeToOnHoldConnection(i)
            already_changed = true
          } else {
            throw new Exception(s"Relation $name should have child node")
          }
        }

      }
    }
  }

  def insert(tuple : Attributes) : Unit = {
    if (glob_info.shouldDebug(tuple)) {
      glob_info.debugContext("insert", s" rel=$name tuple=$tuple testAlive=${testAlive(tuple)} numChild=$numChild")
    }

    insertKeyMap(tuple)
    val c : Int = testAlive(tuple)
    if (c == numChild) {
      for(ui<- upward_infos)
      {
        update_index_tool.aliveAdd(tuple,ui)
      }

    } else {
      for(ui<- upward_infos)
        update_index_tool.onholdAdd(tuple, c,ui)
    }

  }

  def delete(tuple : Attributes) : Unit = {
    if (glob_info.shouldDebug(tuple)) {
      glob_info.debugContext("delete", s" rel=$name tuple=$tuple testAlive=${testAlive(tuple)} numChild=$numChild")
    }

    update_index_tool.deleteKeyMap(tuple)
    val c : Int = testAlive(tuple)
    if (c == numChild) {
      for(ui<- upward_infos)
        update_index_tool.aliveDelete(tuple,ui)
    } else {
      for(ui<- upward_infos)
        update_index_tool.onholdDelete(tuple,ui)
    }

  }

  def CheckWitness(outputs : Attributes, operator : String = "Insert", ui: UpwardInfo):Unit = {

    if(!ui.child_in_connex) return

    val key = outputs.projection(ui.join_key_idx_in_output_attr)
    var valid_queries = ui.related_q;
    if(ui.nextRelation != null)
      valid_queries = ui.nextRelation.ifGloballyAlive(key, ui.conn)

    if (valid_queries.nonEmpty) {
      output_tool.WitnessTupleTriggerEnum(outputs, operator, ui, valid_queries)
    }

  }

  def IsDebugging() : Boolean = {

    false
  }

  def ifGloballyAlive(key : Attributes, t: TupleConnection): Array[QueryId] = {

      if(glob_info.isTargetLine) {
      glob_info.debugContext("ifGloballyAlive", s" rel=$name key=$key ")
      if(t.live_view.get(key) != null)
        println(t.live_view.get(key).keys.toArray.mkString(","))
      else println("return empty")
    }

    if(t.live_view.get(key) != null)
    {

      t.live_view.get(key).keys.toArray
    }
    else {

      Array()
    }
  }

  def GetAliveOutAttrCnt(ui: UpwardInfo, key: Attributes, outAttr: Attributes): Int = {
    if (!ui.alive.contains(key)) return 0
    val innerMap = ui.alive.get(key)
    if (innerMap == null || !innerMap.contains(outAttr)) return 0
    innerMap.get(outAttr).getOrElse(0)
  }

  def changeGlobalAlive(tuple : Attributes, operation : String = "Insert",
                        qids: Array[QueryId],
                        really_on_alive_edit_path: Boolean = false): Unit = {

    if(glob_info.isTargetLine) {
      glob_info.debugContext("changeGlobalAlive", s" rel=$name tuple=$tuple op=$operation qids=[${qids.filter(_!=null).mkString(",")}]")
    }

    if (this.numChild == 0) return

    for(ui <- upward_infos)
    {
      var handling_qs: Array[QueryId] = ui.related_q.intersect(qids)

      if (operation == "Delete" && ui.nextRelation != null)
      {
        val upjoin_key = tuple.projection(ui.join_key_idx_in_output_attr)
        val still_alive_qs = ui.nextRelation.ifGloballyAlive(upjoin_key, ui.conn)

        if (!really_on_alive_edit_path || GetAliveOutAttrCnt(ui, upjoin_key, tuple) != 1) {
          handling_qs = handling_qs.diff(still_alive_qs)
        }
      }
      if(glob_info.isTargetLine) {
        glob_info.debugContext("changeGlobalAlive after filter", s" rel=$name tuple=$tuple op=$operation qids=[${qids.filter(_!=null).mkString(",")}]")
      }

      for (i <- connection) {

        assert(i.parent_jk.forall(output_attr.contains))

        if (operation == "Insert") {
          val j_k = i.parent_jk.map (attr => output_attr.indexOf(attr))
          val key = tuple.projection(j_k)
          update_index_tool.addElementToLiveView(key, tuple, i.live_view, handling_qs)
        } else {
          if (operation == "Delete") {
            val j_k = i.parent_jk.map (attr => output_attr.indexOf(attr))
            val key = tuple.projection(j_k)
            if(glob_info.isTargetLine)
              glob_info.debugContext("before del live view", s" rel=$name tuple=$tuple key=$key qids=[${qids.filter(_!=null).mkString(",")}] handling_qs=[${handling_qs.mkString(",")}]")
            update_index_tool.deleteElementFromLiveView(key, tuple, i.live_view, handling_qs)
          }
        }
      }
    }

  }

  override def toString: String = {
    "Relation " + name
  }

}

object Relation
{

}
