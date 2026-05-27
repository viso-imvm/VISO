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

class UpdateIndexTool(rel:Relation) {

  def addElementToIndex(key : Attributes, tuple : Attributes, state : MapState[Attributes, mutable.HashSet[Attributes]]) : Unit = {
    if (rel.glob_info.shouldDebug(key) || rel.glob_info.shouldDebug(tuple)) {
      rel.glob_info.debugContext("addElementToIndex", s" rel=${rel.name} key=$key tuple=$tuple contains=${state.contains(key)}")
    }
    if (state.contains(key)) {
      val temp = state.get(key)
      temp.add(tuple)
    } else {
      val temp = new mutable.HashSet[Attributes]()
      temp.add(tuple)
      state.put(key, temp)
    }
  }

  def addElementToLiveView(key: Attributes, tuple: Attributes, state: MapState[Attributes, mutable.HashMap[QueryId,mutable.HashSet[Attributes]]], qids: Array[QueryId]): Unit = {

    if (state.contains(key)) {
      val temp = state.get(key)
      for(qid <- qids)
      {
        rel.util_tool.AssertInConnex(qid)

          if(!temp.contains(qid))
          {
            val newSet = new mutable.HashSet[Attributes]()
            newSet.add(tuple)
            temp.put(qid, newSet)

            if(rel.glob_info.isTargetLine) {
              rel.glob_info.debugContext("LiveView add", s" rel=${rel.name} key=$key tuple=$tuple qids=[${qids.filter(_!=null).mkString(",")}] isNewKey=true")
            }
          }
          else
          {
            val existingSet = temp(qid)
            existingSet.add(tuple)
          }

      }
    } else {
      val temp = new mutable.HashMap[QueryId,mutable.HashSet[Attributes]]()
      for(qid <- qids)
      {
        rel.util_tool.AssertInConnex(qid)

        val newSet = new mutable.HashSet[Attributes]()
        newSet.add(tuple)
        temp.put(qid, newSet)

        if(rel.glob_info.isTargetLine) {
          rel.glob_info.debugContext("LiveView add", s" rel=${rel.name} key=$key tuple=$tuple qids=[${qids.filter(_!=null).mkString(",")}] isNewKey=true")
        }

      }
      state.put(key, temp)
    }
  }

  def deleteElementFromLiveView(key: Attributes, tuple: Attributes, state: MapState[Attributes, mutable.HashMap[QueryId, mutable.HashSet[Attributes]]], qids: Array[QueryId]): Unit = {

    if(rel.glob_info.isTargetLine) {
      rel.glob_info.debugContext("before del liveview", s" rel=${rel.name} key=$key delTuple=$tuple qids=[${qids.filter(_!=null).mkString(",")}]")
      if(state.contains(key)) {
        val kv = state.get(key)
        println(s"  keyExists=true, innerMap keys=[${kv.keys.map(k=>k.toString).mkString(",")}]")
        kv.foreach { case (qid, tset) => println(s"    qid=$qid tuples=[${tset.map(t=>t.toString).mkString("; ")}]" )}
      } else { println("  keyExists=false") }
    }

    if (state.contains(key)) {
      val temp = state.get(key)
      var shouldRemoveKey = true

      for (qid <- qids) {
        rel.util_tool.AssertInConnex(qid)
        if (temp.contains(qid)) {
          val existingSet = temp(qid)

          if(rel.glob_info.isTargetLine) {
            println(s"  [del qid=$qid] before: size=${existingSet.size}, containsTuple=${existingSet.contains(tuple)}")
          }

          existingSet.remove(tuple)

          if(rel.glob_info.isTargetLine) {
            println(s"  [del qid=$qid] after: size=${existingSet.size}")
          }

          if (existingSet.nonEmpty) {
            shouldRemoveKey = false
          } else {

            temp.remove(qid)

            if(rel.glob_info.isTargetLine) {
              rel.glob_info.debugContext("LiveView delete", s" rel=${rel.name} key=$key tuple=$tuple qids=[${qids.filter(_!=null).mkString(",")}]")
            }
          }
        }

      }

      if (shouldRemoveKey && temp.isEmpty) {
        state.remove(key)
      } else if (!temp.isEmpty) {

        state.put(key, temp)
      }

      if(rel.glob_info.isTargetLine) {
        println(s"[after del liveview] shouldRemoveKey=$shouldRemoveKey temp.isEmpty=${temp.isEmpty}")
        if(state.contains(key)) {
          val kv2 = state.get(key)
          println(s"  final key still exists! innerMap keys=[${kv2.keys.map(k=>k.toString).mkString(",")}]")
          kv2.foreach { case (qid, tset) => println(s"    qid=$qid tuples=[${tset.map(t=>t.toString).mkString("; ")}]" )}
        } else { println("  final key removed from state") }
      }
    }
  }

  def addElementToAlive(key : Attributes, tuple : Attributes, state : MapState[Attributes, mutable.HashMap[Attributes,Int]]) : Boolean = {

    val innerMap = if (!state.contains(key)) {
      val newMap = new mutable.HashMap[Attributes, Int]()
      state.put(key, newMap)
      newMap
    } else {
      state.get(key)
    }

    val currentCount = innerMap.getOrElse(tuple, 0)
    innerMap.update(tuple, currentCount + 1)
    return currentCount == 0
  }

  def deleteElementFromIndex(key : Attributes, tuple : Attributes, state : MapState[Attributes, mutable.HashSet[Attributes]]) : Unit = {
    if (rel.glob_info.shouldDebug(key) || rel.glob_info.shouldDebug(tuple)) {
      rel.glob_info.debugContext("deleteElementFromIndex", s" rel=${rel.name} key=$key tuple=$tuple contains=${state.contains(key)}")
    }
    if (state.contains(key)) {
      val temp = state.get(key)
      if (!temp.remove(tuple))
        throw new Exception("Element is not found in the state " + tuple.toString)
      if (temp.isEmpty)
      {
        state.remove(key)
      }
    }
    else {
      if (rel.glob_info.shouldDebug(key) || rel.glob_info.shouldDebug(tuple)) {
        rel.glob_info.debugContext("deleteElementFromIndex FAIL", s" rel=${rel.name} key=$key tuple=$tuple KEY NOT in state. Dumping all keys...")
        import scala.collection.JavaConverters._
        for (k <- state.keys().asScala) {
          println(s"    state key: $k -> set size: ${state.get(k).size}")
        }
      }
      throw new Exception("Element " + tuple.toString +" with key "+ key.toString + " not in state " + rel.name)
    }
  }

  def deleteElementFromIndexMap(key : Attributes, tuple : Attributes, state : MapState[Attributes, mutable.HashMap[Attributes,Int]]) : Unit = {

    if (!state.contains(key)) {
      return
    }

    val innerMap = state.get(key)

    innerMap.get(tuple) match {
      case Some(count) =>
        if (count > 1) {
          innerMap.update(tuple, count - 1)
        } else {
          innerMap.remove(tuple)

          if (innerMap.isEmpty) {
            state.remove(key)
          }
        }
      case None =>

    }
  }

  def deleteKeyMap(tuple : Attributes) : Unit = {
    val c : Int = rel.testAlive(tuple)
    if (rel.glob_info.shouldDebug(tuple)) {
      rel.glob_info.debugContext("deleteKeyMap", s" rel=${rel.name} tuple=$tuple testAlive=$c numChild=${rel.numChild}")
    }
    for (i <- rel.connection) {
      val keys = tuple.projection(i.parent_jk)
      if (c == rel.numChild) {
        deleteElementFromIndex(keys, tuple, i.k2alive)
      } else {
        deleteElementFromIndex(keys, tuple, i.k2onhold)
      }
    }
  }

  def changeToAliveConnection(tuple : Attributes) : Unit = {
    for (i <- rel.connection) {
      val keys = tuple.projection(i.parent_jk)
      if (rel.glob_info.shouldDebug(tuple) || rel.glob_info.shouldDebug(keys)) {
        rel.glob_info.debugContext("changeToAliveConnection", s" rel=${rel.name} child_rel=${i.child_rel.name} tuple=$tuple keys=$keys k2onhold_contains=${i.k2onhold.contains(keys)} k2alive_contains=${i.k2alive.contains(keys)}")
      }
      addElementToIndex(keys, tuple, i.k2alive)
      deleteElementFromIndex(keys, tuple, i.k2onhold)
    }
  }

  def changeToOnHoldConnection(tuple : Attributes) : Unit = {
    for (i <- rel.connection) {
      val keys = tuple.projection(i.parent_jk)
      if (rel.glob_info.shouldDebug(tuple) || rel.glob_info.shouldDebug(keys)) {
        rel.glob_info.debugContext("changeToOnHoldConnection", s" rel=${rel.name} child_rel=${i.child_rel.name} tuple=$tuple keys=$keys k2alive_contains_key=${i.k2alive.contains(keys)} k2onhold_contains_key=${i.k2onhold.contains(keys)}")
        if (i.k2alive.contains(keys)) {
          println(s"    k2alive[$keys] = ${i.k2alive.get(keys).mkString(", ")}")
        }
      }
      addElementToIndex(keys, tuple, i.k2onhold)
      deleteElementFromIndex(keys, tuple, i.k2alive)
    }
  }

  def aliveAdd(tuple : Attributes, ui: UpwardInfo) : Unit = {
      var key_new : Boolean = false
      var output_attr_new : Boolean = false
      val projectB = tuple.projection(ui.joinkey)
      if (rel.glob_info.shouldDebug(tuple) || rel.glob_info.shouldDebug(projectB)) {
        rel.glob_info.debugContext("aliveAdd", s" rel=${rel.name} tuple=$tuple projectB=$projectB keyCount_before=${if(ui.keyCount.contains(projectB)) ui.keyCount.get(projectB) else "NEW"} nextRel=${if(ui.nextRelation!=null) ui.nextRelation.name else "null"}")
      }
      if (ui.alive.contains(projectB)) {
        assert (ui.keyCount.contains(projectB))

        val tempCount = ui.keyCount.get(projectB)
        ui.keyCount.put(projectB, tempCount+1)
      } else {
        assert (!ui.keyCount.contains(projectB))
        key_new = true
        ui.keyCount.put(projectB, 1)

      }

      val outputs = tuple.projection(rel.output_attr)
      output_attr_new = addElementToAlive(projectB, outputs, ui.alive)

      if(key_new && ui.nextRelation!=null)
      {
        ui.nextRelation.updateAlive(projectB, ui.conn)
      }
      else
      {
        if(output_attr_new)
          rel.CheckWitness(outputs, "Insert",ui)
      }

  }

  def onholdDelete(tuple : Attributes, ui:UpwardInfo) : Unit = {

      val projectB = tuple.projection(ui.joinkey)
      if (rel.glob_info.shouldDebug(tuple) || rel.glob_info.shouldDebug(projectB)) {
        rel.glob_info.debugContext("onholdDelete", s" rel=${rel.name} tuple=$tuple projectB=$projectB nextRel=${if(ui.nextRelation!=null) ui.nextRelation.name else "null"}")
      }
      if (ui.onhold.contains(projectB)) {
        val temp = ui.onhold.get(projectB)
        if (!temp.contains(tuple))
          throw new Exception("Element is not found in the state")
        else {
          temp.remove(tuple)
          if (temp.isEmpty)
            ui.onhold.remove(projectB)
        }
      } else {
        throw new Exception("ERROR! Element not found in onhold "+ rel.name + " " + tuple.toString)
      }

  }

  def aliveDelete(tuple : Attributes, ui:UpwardInfo) : Unit = {

      ui.on_alive_edit_path = true

      if (rel.glob_info.shouldDebug(tuple)) {
        val projectB = tuple.projection(ui.joinkey)
        rel.glob_info.debugContext("aliveDelete", s" rel=${rel.name} tuple=$tuple projectB=$projectB keyCount=${ui.keyCount.get(projectB)} nextRel=${if(ui.nextRelation!=null) ui.nextRelation.name else "null"}")
      }

      val projectB = tuple.projection(ui.joinkey)

      assert (ui.keyCount.contains(projectB))
      var key_del:Boolean = false
      var output_attr_del:Boolean = false

      val tempCount = ui.keyCount.get(projectB)
      if (tempCount == 1)
      {

        ui.keyCount.remove(projectB)
        key_del = true

      }
      else
        ui.keyCount.put(projectB, tempCount-1)

      val outputs = tuple.projection(rel.output_attr)
      if(ui.alive.get(projectB).get(outputs).get == 1)
        output_attr_del = true

      if(key_del && ui.nextRelation != null)
      {
        ui.nextRelation.updateOnHold(projectB, ui.conn)
      }
      else
      {
        if(output_attr_del)
          rel.CheckWitness(outputs, "Delete",ui)
      }

      deleteElementFromIndexMap(projectB,outputs,ui.alive)

      ui.on_alive_edit_path = false

  }

  def onholdAdd(tuple : Attributes, st : Int, ui:UpwardInfo) : Unit = {

      val projectB = tuple.projection(ui.joinkey)
      if (rel.glob_info.shouldDebug(tuple) || rel.glob_info.shouldDebug(projectB)) {
        rel.glob_info.debugContext("onholdAdd", s" rel=${rel.name} tuple=$tuple projectB=$projectB st=$st nextRel=${if(ui.nextRelation!=null) ui.nextRelation.name else "null"}")
      }
      if (ui.onhold.contains(projectB)) {
        val temp = ui.onhold.get(projectB)
        if (temp.contains(tuple))
          throw new Exception("ERROR! Element already added " + tuple.toString+" "+rel.name)
        else {
          temp.put(tuple, st)
        }
      } else {
        val temp = new mutable.HashMap[Attributes, Int]()
        temp.put(tuple, st)
        ui.onhold.put(projectB, temp)
      }

  }

}
