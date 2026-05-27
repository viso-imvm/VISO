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

class fullAttrEnumeratorModified(rel:Relation, tuple: Attributes, operation: String = "Insert", ui: UpwardInfo, qs: Array[QueryId]) extends Iterator[(Int, Attributes)] {

    if(rel.glob_info.isTargetLine) {
      println(s"[DEBUG fullAttrEnum] fileLine=${rel.glob_info.currentFileLine} rel=${rel.name} tuple=${tuple.toString} op=$operation")
    }

    private var childIterators: List[Iterator[Attributes]] = _
    private var currentChildAttributes: Array[Attributes] = _
    private var child_valid_idxs : Array[Int] = _

    private var baseIterator: Iterator[(Array[QueryId],Attributes)] = _
    private var currentBaseAttribute: Attributes = _
    private var currentQidSet: Array[QueryId] = _
    private var cur_qid_idx: Int = 0

    private var rootBaseIter: Iterator[Attributes] = _

    private var isInitialized: Boolean = false
    private var isExhausted: Boolean = false

    private var really_on_alive_edit_path: Boolean = false

    private def initialChild(r: TupleConnection, parentTuple: Attributes): (Iterator[Attributes], Attributes) = {
      if(!r.ui.child_in_connex) return (Iterator.empty, null)
      val proj_key = r.parent_jk.map (attr => rel.output_attr.indexOf(attr))
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
            println(rel.name + " fullEnumeratorModified with keys: " + tuple.toString)
            println(qs.mkString(","))
        }

        isInitialized = true

        really_on_alive_edit_path = (operation == "Delete") && ui.on_alive_edit_path

        if (ui.nextRelation == null) {

            if (ui.alive.isEmpty) {
                assert(false)
                return
            }
            rootBaseIter = new joinKeyEnumeratorModified(rel,tuple.projection(ui.join_key_idx_in_output_attr), operation, ui,qs, really_on_alive_edit_path)

            if (rootBaseIter.hasNext) {

                currentQidSet = qs
                currentBaseAttribute = rootBaseIter.next()

            } else {
                assert(false)
                return
            }

            return
        }

        if(rel.name == "generalized1" && tuple.values(0) == "36") println("name: " + rel.name + " fullAttrEnumeratorModified initialize called for non-root relation with tuple "+tuple.toString+" and operation "+operation)
        rel.changeGlobalAlive(tuple, operation, qs, really_on_alive_edit_path)
        val j_k = ui.join_key_idx_in_output_attr
        val joinKey = tuple.projection(j_k)

        val stableNextRelation = fullAttrEnumeratorModified.this.ui.nextRelation
        baseIterator = new deltaEnumeratorModified(stableNextRelation,joinKey, ui.conn, "Delta",qs)

        if (baseIterator.hasNext) {

            val nextValue: (Array[QueryId], Attributes) = baseIterator.next()
            currentQidSet = nextValue._1
            currentBaseAttribute = nextValue._2
        } else {
            assert(false)
            return
        }

        val indexedConnections = rel.connection.zipWithIndex
        val processed = indexedConnections.map { case (r, idx) =>
          val (it, attr) = initialChild(r, tuple)
          (it, attr, idx, attr == null)
        }
        val (validItems, skippedIndices) = processed.partition(!_._4)
        childIterators = validItems.map(t => t._1).toList
        currentChildAttributes = validItems.map(t => t._2).toArray
        child_valid_idxs = validItems.map(t => t._3).toArray

        if (currentChildAttributes.contains(null) && rel.connection.nonEmpty) {
            throw new Exception("fullAttrEnumeratorModified: Child iterator is empty at initialization for relation "+rel.name)
        }
    }

    override def hasNext: Boolean = {
        if (!isInitialized) initialize()
        !isExhausted
    }

    override def next(): (Int, Attributes) = {

        if (!hasNext) throw new NoSuchElementException()

        if(ui.nextRelation == null) {

          val nextQid = currentQidSet(cur_qid_idx)

          var nextResult = currentBaseAttribute.projection(rel.output_project_info(nextQid))

          cur_qid_idx += 1
          if(cur_qid_idx == currentQidSet.length) {

            cur_qid_idx = 0
            isExhausted = !rootBaseIter.hasNext
            if(hasNext) {

              currentBaseAttribute = rootBaseIter.next()
            }
          }
          val qid_int = nextQid.qid
          return (qid_int, nextResult)

        }

        var nextResult = tuple.join(currentBaseAttribute)
        for (childAttr <- currentChildAttributes) {
            nextResult = nextResult.join(childAttr)
        }

        val nextQid = currentQidSet(cur_qid_idx)
        var projectedResult = nextResult.projection(rel.output_project_info(nextQid))

        cur_qid_idx += 1

        if(cur_qid_idx == currentQidSet.length) {
            cur_qid_idx = 0

            if (baseIterator.hasNext) {
                val nextValue: (Array[QueryId], Attributes) = baseIterator.next()
                currentQidSet = nextValue._1
                currentBaseAttribute = nextValue._2

            } else {

                var advanced = false
                for (i <- childIterators.indices.reverse if !advanced) {
                    if (childIterators(i).hasNext) {
                        currentChildAttributes(i) = childIterators(i).next()
                        advanced = true
                    } else {

                        val r = rel.connection(child_valid_idxs(i))
                        val (newIter, newAttr) = initialChild(r, tuple)
                        childIterators = childIterators.updated(i, newIter)
                        currentChildAttributes(i) = newAttr
                        if (newAttr == null) {
                            isExhausted = true
                            advanced = true
                        }
                    }
                }

                if (advanced && !isExhausted) {

                    val joinKey = tuple.projection(ui.join_key_idx_in_output_attr)
                    val stableNextRelation = fullAttrEnumeratorModified.this.ui.nextRelation
                    baseIterator = new deltaEnumeratorModified(stableNextRelation, joinKey, ui.conn, "Delta", qs)
                    if (baseIterator.hasNext) {
                        val nextValue: (Array[QueryId], Attributes) = baseIterator.next()
                        currentQidSet = nextValue._1
                        currentBaseAttribute = nextValue._2
                    } else {
                        isExhausted = true
                    }
                } else if (!advanced) {
                    isExhausted = true
                }
            }
        }
        val qid_int = nextQid.qid

        (qid_int, projectedResult)

    }
}
