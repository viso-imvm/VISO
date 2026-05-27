package RelationType.Enumerator
import RelationType.Attributes
import RelationType.Relation
import RelationType.Util.{UpwardInfo,QueryId,TupleConnection}
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

class joinKeyEnumeratorModified(rel: Relation, keys: Attributes, operation: String = "Delta", ui:UpwardInfo, qs: Array[QueryId], parent_really_on_alive_edit_path: Boolean = false) extends Iterator[Attributes] {

    if(rel.glob_info.isTargetLine) {
      println(s"[DEBUG joinKeyEnum] fileLine=${rel.glob_info.currentFileLine} rel=${rel.name} keys=${keys.toString} op=$operation")
    }

    private var really_on_alive_edit_path: Boolean = false

    private var baseIterator: Iterator[Attributes] = _
    private var currentBase: Attributes = _
    private var childIterators: List[Iterator[Attributes]] = _
    private var currentChildAttributes: Array[Attributes] = _
    private var child_valid_idxs : Array[Int] = _
    private var isInitialized: Boolean = false
    private var isExhausted: Boolean = false

    private def initialChild(r: TupleConnection, parentTuple: Attributes): (Iterator[Attributes], Attributes) = {

      if(!r.ui.child_in_connex) return (Iterator.empty, null)
      val proj_key = r.parent_jk.map(attr => rel.output_attr.indexOf(attr))

      val projectionKey = parentTuple.projection(proj_key)
      val child_ui = r.ui
      var child_qs = rel.util_tool.QidTransform(qs,r)

      val iter = new joinKeyEnumeratorModified(r.child_rel,projectionKey, operation,child_ui,child_qs,really_on_alive_edit_path)

      val current = if (iter.hasNext) iter.next() else null
      (iter, current)
    }

    private def initialize(): Unit = {
      assert(qs.filter(_ != null).nonEmpty)

      if(rel.IsDebugging())
      {
        println(rel.name + " joinKeyEnumeratorModified with keys: " + keys.toString)
        println(qs.mkString(","))
      }

      isInitialized = true

      really_on_alive_edit_path = ui.on_alive_edit_path && parent_really_on_alive_edit_path

      baseIterator = if (keys == null) {
        throw new NoSuchMethodException("For joinKey enumeration, keys cannot be empty")
      }
      else {
        if(!ui.alive.contains(keys))
        {
          println("name: " + rel.name + " keys: "+keys.toString+ " joinKeyEnumeratorModified alive keys:")

          ui.alive.keys().asScala.foreach(key => println(key))
          throw new NoSuchMethodException("joinKeyEnumeratorModified: keys not in alive state for relation "+rel.name)
        }
        ui.alive.get(keys).keysIterator
      }

      if (baseIterator.hasNext) {

        currentBase = baseIterator.next()

        rel.changeGlobalAlive(currentBase, operation, qs, really_on_alive_edit_path)
      } else {
        isExhausted = true
        return
      }

      val indexedConnections = rel.connection.zipWithIndex
      val processed = indexedConnections.map { case (r, idx) =>
        val (it, attr) = initialChild(r, currentBase)
        (it, attr, idx, attr == null)
      }
      val (validItems, skippedIndices) = processed.partition(!_._4)
      childIterators = validItems.map(t => t._1).toList
      currentChildAttributes = validItems.map(t => t._2).toArray
      child_valid_idxs = validItems.map(t => t._3).toArray

      if (currentChildAttributes.contains(null) && rel.connection.nonEmpty) {
        assert(false)
        throw new Exception("joinKeyEnumeratorModified: Child iterator is empty at initialization for relation "+rel.name)
      }
    }

    override def hasNext: Boolean = {
      if (!isInitialized) initialize()
      !isExhausted
    }

    override def next(): Attributes = {

      if (!hasNext) {
        println("name: " + rel.name + " joinKeyEnumeratorModified no next")
        throw new NoSuchElementException()
      }

      var result = currentBase

      for (childAttr <- currentChildAttributes) {
        result = result.join(childAttr)
      }

      var advanced = false
      for (i <- childIterators.indices.reverse if !advanced) {
        if (childIterators(i).hasNext) {

          currentChildAttributes(i) = childIterators(i).next()

          advanced = true
        } else {

          val r = rel.connection(child_valid_idxs(i))

          val (newIter, newAttr) = initialChild(r, currentBase)
          childIterators = childIterators.updated(i, newIter)
          currentChildAttributes(i) = newAttr

          if (newAttr == null) {
            throw new Exception("joinKeyEnumeratorModified: Child iterator is empty after reset for relation "+rel.name)
          }

        }
      }

      if (!advanced) {
        if (baseIterator.hasNext) {

          currentBase = baseIterator.next()
          if(rel.name == "generalized1" && currentBase.values(0) == "36") println("name: " + rel.name + " joinKeyEnumeratorModified advanced currentBase: "+currentBase.toString)
          rel.changeGlobalAlive(currentBase, operation, qs, really_on_alive_edit_path)

          val childInitials = rel.connection.map(r => initialChild(r, currentBase)).filterNot(_._2==null)
          childIterators = childInitials.map(_._1).toList
          currentChildAttributes = childInitials.map(_._2).toArray

          if (currentChildAttributes.contains(null) && rel.connection.nonEmpty) {
            isExhausted = true
          }
        }
        else {
          isExhausted = true
        }
      }

      result
    }
  }
