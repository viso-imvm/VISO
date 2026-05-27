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

  class deltaEnumeratorModified(rel:Relation, keys: Attributes, val ret: TupleConnection, operation: String = "Insert", qs: Array[QueryId]) extends Iterator[(Array[QueryId],Attributes)] {

    if(rel.glob_info.isTargetLine) {
      println(s"[DEBUG deltaEnum] fileLine=${rel.glob_info.currentFileLine} rel=${rel.name} keys=${keys.toString} op=$operation child=${ret.child_rel.name}")
    }

    private var currentBase: Attributes = _
    private var currentQidSet: Array[QueryId] = _
    private var childIterators: List[Iterator[Attributes]] = List.empty
    private var currentChildAttributes: Array[Attributes] = Array.empty
    private var child_valid_idxs: Array[Int] = _
    private var isInitialized: Boolean = false
    private var isExhausted: Boolean = false

    private var localTuples: mutable.HashMap[Attributes, Array[QueryId]] = _
    private var localTuplesIter: Iterator[(Attributes, Array[QueryId])] = _
    private var currentLocalTuple: (Attributes, Array[QueryId]) = _

    private var nestedLoopJoinIter: NestedLoopJoinIterator = _

    private def initialChild(r: TupleConnection, parentTuple: Attributes): (Iterator[Attributes], Attributes) = {
        if(!r.ui.child_in_connex) return (Iterator.empty, null)
        val proj_key = r.parent_jk.map (attr => rel.output_attr.indexOf(attr))
        val projectionKey = parentTuple.projection(proj_key)
        val child_ui = r.ui
        var child_qs = rel.util_tool.QidTransform(qs,r)

        val iter = new joinKeyEnumeratorModified(r.child_rel,projectionKey, "Delta",child_ui,child_qs)
        val current = if (iter.hasNext) iter.next() else null
        (iter, current)
    }

    private def initialize(): Unit = {
        assert(qs.filter(_ != null).nonEmpty)
        isInitialized = true

        if (keys == null) throw new NoSuchMethodException("For Delta enumeration, keys cannot be empty")

        if(!ret.live_view.contains(keys))
        {
          println("keys: "+keys.toString)
          println("currentfileLine: " + rel.glob_info.currentFileLine)
          println(s"currentOperation: ${rel.glob_info.currentOperation}")
          println(s"currentTuple: ${rel.glob_info.currentTuple}")
          println(s"currentRelationName: ${rel.glob_info.currentRelationName}")
          throw new NoSuchMethodException("DeltaEnumeratorModified: Key not found in the local index for relation "+rel.name)
        }

        localTuples = rel.util_tool.GetUnion(qs, ret, keys)
        if (localTuples.isEmpty) {
          println("local tuples empty: "+ rel.name+" "+ret.child_rel.name +" "+ keys)
          assert(false)
          return
        }

        localTuplesIter = localTuples.iterator
        if (!localTuplesIter.hasNext) {
          assert(false)
          return
        }

        currentLocalTuple = localTuplesIter.next()
        nestedLoopJoinIter = new NestedLoopJoinIterator(currentLocalTuple)

        if (nestedLoopJoinIter.hasNext) {
            val nextValue = nestedLoopJoinIter.next()
            currentBase = nextValue._1
            currentQidSet = nextValue._2
        } else {
            assert(false, "local tuple has no parent and no root ui — should not happen")
            return
        }

        initializeChildIterators(rel.connection.filterNot(_ == ret).toArray, currentLocalTuple._1)
    }

    private class NestedLoopJoinIterator(
        localTuple: (Attributes, Array[QueryId])
    ) extends Iterator[(Attributes, Array[QueryId])] {

        val currentLocalTuple: (Attributes, Array[QueryId]) = localTuple

        private val uiAndQsList: List[(UpwardInfo, Array[QueryId])] = {
            val buf = mutable.ListBuffer[(UpwardInfo, Array[QueryId])]()
            for (ui_tmp <- rel.upward_infos) {
                val handling_qs = ui_tmp.related_q.intersect(currentLocalTuple._2)
                if (handling_qs.nonEmpty) {
                    buf.append((ui_tmp, handling_qs))
                }
            }
            buf.toList
        }

        private var uiIdx: Int = 0
        private var currentParentIterator: Iterator[(Array[QueryId], Attributes)] = _

        private var currentResult: Attributes = _
        private var currentQid: Array[QueryId] = _
        private var started: Boolean = false

        private def switchToNextUi(): Boolean = {
            while (uiIdx < uiAndQsList.length) {
                val (ui_cur, sub_qs) = uiAndQsList(uiIdx)
                uiIdx += 1
                if (ui_cur.nextRelation == null) {

                    currentParentIterator = null
                    currentResult = currentLocalTuple._1
                    currentQid = sub_qs
                    return true
                } else {

                    val parentJoinKey = currentLocalTuple._1.projection(ui_cur.join_key_idx_in_output_attr)
                    currentParentIterator = new deltaEnumeratorModified(
                        ui_cur.nextRelation, parentJoinKey, ui_cur.conn, "Delta", sub_qs
                    )
                    if (currentParentIterator.hasNext) {
                        val parentTuple = currentParentIterator.next()
                        currentResult = parentTuple._2.join(currentLocalTuple._1)
                        currentQid = parentTuple._1
                        return true
                    }

                }
            }
            false
        }

        private def advance(): Boolean = {
            if (!started) {
                started = true
                return switchToNextUi()
            }

            if (currentParentIterator != null && currentParentIterator.hasNext) {
                val parentTuple = currentParentIterator.next()
                currentResult = parentTuple._2.join(currentLocalTuple._1)
                currentQid = parentTuple._1
                return true
            }

            switchToNextUi()
        }

        override def hasNext: Boolean = {
            if (currentResult != null) true else advance()
        }

        override def next(): (Attributes, Array[QueryId]) = {
            if (!hasNext) throw new NoSuchElementException()
            val result = currentResult
            val qid = currentQid
            currentResult = null
            (result, qid)
        }
    }

    private def initializeChildIterators(connections: Array[TupleConnection], parentTuple: Attributes): Unit = {
        val indexedConnections = connections.zipWithIndex
        val processed = indexedConnections.map { case (r, idx) =>
            val (it, attr) = initialChild(r, parentTuple)
            (it, attr, idx, attr == null)
        }

        val validItems = processed.filter(!_._4)
        childIterators = validItems.map(_._1).toList
        currentChildAttributes = validItems.map(_._2).toArray
        child_valid_idxs = validItems.map(_._3).toArray

        if (currentChildAttributes.contains(null) && rel.connection.nonEmpty) {
            assert(false)
        }
    }

    override def hasNext: Boolean = {
        if (!isInitialized) initialize()
        !isExhausted
    }

    override def next(): (Array[QueryId],Attributes) = {
       if (!hasNext) throw new NoSuchElementException()

        var result = currentBase
        var qidSet = currentQidSet
        for (childAttr <- currentChildAttributes) {
            result = result.join(childAttr)
        }

        advanceIterators()
        (rel.util_tool.QidTransform(qidSet,ret),result)
    }

    private def advanceIterators(): Unit = {

        if (nestedLoopJoinIter.hasNext) {
            val nextValue = nestedLoopJoinIter.next()
            currentBase = nextValue._1
            currentQidSet = nextValue._2
            return
        }

        var childAdvanced = false
        for (i <- childIterators.indices.reverse if !childAdvanced) {
            if (childIterators(i).hasNext) {
                currentChildAttributes(i) = childIterators(i).next()
                childAdvanced = true
            } else {

                val connections = rel.connection.filterNot(_ == ret)
                val r = connections(child_valid_idxs(i))
                val (newIter, newAttr) = initialChild(r, currentLocalTuple._1)
                childIterators = childIterators.updated(i, newIter)
                currentChildAttributes(i) = newAttr
                if (newAttr == null) {
                    assert(false)
                }
            }
        }

        if (childAdvanced) {

            nestedLoopJoinIter = new NestedLoopJoinIterator(currentLocalTuple)
            if (nestedLoopJoinIter.hasNext) {
                val nextValue = nestedLoopJoinIter.next()
                currentBase = nextValue._1
                currentQidSet = nextValue._2
            } else {
                assert(false, "local tuple has no parent and no root ui — should not happen")
            }
            return
        }

        if (localTuplesIter.hasNext) {
            currentLocalTuple = localTuplesIter.next()
            initializeChildIterators(rel.connection.filterNot(_ == ret).toArray, currentLocalTuple._1)
            nestedLoopJoinIter = new NestedLoopJoinIterator(currentLocalTuple)
            if (nestedLoopJoinIter.hasNext) {
                val nextValue = nestedLoopJoinIter.next()
                currentBase = nextValue._1
                currentQidSet = nextValue._2
            } else {
                assert(false, "local tuple has no parent and no root ui — should not happen")
            }
            return
        }

        isExhausted = true
    }
  }
