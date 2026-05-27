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
import javax.management.Query
import org.apache.flink.api.java.tuple.Tuple

class InitTool(rel:Relation)
{

  def collectFullSchema(query_id: QueryId): Array[(Relation, Int, Int)] = {
    println(s"$rel.name: start Collecting full schema for QueryId(qid=${query_id.qid}, place_id=${query_id.place_id})")

    var parent: Relation = rel.util_tool.FindParentRel(query_id)
    var includeAncestors: Boolean = (parent != null)

    val fullSchema = new ArrayBuffer[(Relation, Int, Int)]()

    def collectDescendants(relation: Relation, rel_place_id: Int, skip_conn: TupleConnection = null): Unit = {
        if(rel.IsDebugging())
          println(s"$rel.name: Collecting descendants of relation ${relation.name} with place_id $rel_place_id")
        for (conn <- relation.connection) {
            val childRelation = conn.child_rel
            if(conn != skip_conn) {
              val parent_qid : QueryId = new QueryId()
              parent_qid.qid = query_id.qid
              parent_qid.place_id = rel_place_id
              val child_qid = conn.p2c_qid.get(parent_qid)

              for (attr <- childRelation.util_tool.GetOutputAttr(child_qid)) {
                fullSchema += ((childRelation,child_qid.place_id, attr))
              }

              collectDescendants(childRelation, child_qid.place_id)
            }

        }
    }

    if (includeAncestors) {

        for (attr <- rel.util_tool.GetOutputAttr(query_id)) {
            fullSchema += ((rel, query_id.place_id, attr))
        }

        var current: (Relation,Int,TupleConnection) = (rel, query_id.place_id,null)
        val ancestors = new ArrayBuffer[(Relation,Int, TupleConnection)]()

        var cur_parent: (Relation,Int,TupleConnection) = (rel, query_id.place_id,null)
        while (cur_parent != null) {
          val parent_qid = new QueryId()
          parent_qid.qid = query_id.qid
          parent_qid.place_id = current._2

          cur_parent = current._1.util_tool.FindParentRelWithPId(parent_qid)
          if(cur_parent != null) {
            ancestors += ((cur_parent._1,cur_parent._2, cur_parent._3))
          }

          current = cur_parent
        }

        ancestors.reverse.foreach { case (ancestor: Relation,ancestor_place_id: Int, conn: TupleConnection) =>

            val ancestor_query_id = new QueryId()
            ancestor_query_id.qid = query_id.qid
            ancestor_query_id.place_id = ancestor_place_id

            for (attr <- ancestor.util_tool.GetOutputAttr(ancestor_query_id)) {
                fullSchema += ((ancestor,ancestor_place_id, attr))
            }
            if(rel.IsDebugging())
              println(s"$rel.name: Collected attributes from ancestor relation ${ancestor.name} with place_id $ancestor_place_id")
            collectDescendants(ancestor,ancestor_place_id,conn)
        }
    }
    else
    {

      for (attr <- rel.util_tool.GetOutputAttr(query_id)) {
        fullSchema += ((rel,query_id.place_id, attr))
      }
    }
    println(s"$rel.name: Collecting attributes from descendants...")
    collectDescendants(rel,query_id.place_id)
    println(s"$rel.name: Collected full schema with ${fullSchema.length} attributes for QueryId(qid=${query_id.qid}, place_id=${query_id.place_id})")
    fullSchema.toArray
  }

  def InitOutputProjectInfo(): Unit = {

    var related_query_ids = new ArrayBuffer[QueryId]()

    for(i <- rel.upward_infos)
    {

      related_query_ids = related_query_ids ++ i.related_q
    }
    println(s"$rel.name: Related query IDs: ${related_query_ids.map(qid => s"(qid=${qid.qid}, place_id=${qid.place_id})").mkString(", ")}")

    for(qid <- related_query_ids)
    {
      if(rel.util_tool.FindUpwardInfo(qid).child_in_connex)
      {
        if(rel.name == "tag1")
          println("tag1 qid: "+qid.toString+" "+rel.util_tool.FindUpwardInfo(qid).child_in_connex)

        val fullSchema = collectFullSchema(qid)

        fullSchema.zipWithIndex.foreach { case ((rel, placeid, attr), idx) =>
            println(s"  [$idx] ${rel.name}.$placeid.$attr")
        }

        var projectionMapping: Array[Int] = rel.glob_info.output_result_schema(qid.qid).map { case (rel, place_id, attrName) =>
            val index = fullSchema.indexWhere { case (r, pid, name) =>
                r == rel && name == attrName && pid == place_id
            }
            if (index == -1) {

              fullSchema.foreach { case (rel, pid, attr) =>

              }
              throw new IllegalArgumentException(s"Attribute $attrName from relation ${rel.name} not found in full schema of ${rel.name}")
            }
            index
        }
        println("here rel: "+rel)
        rel.output_project_info.put(qid, projectionMapping)

      }
    }

  }

  def initState(runtimeContext : RuntimeContext) : Unit = {

    rel.runtime = runtimeContext

  }

  def setOut(t : Collector[String], timet : ValueState[Long] = null) : Unit = {
    rel.out = t
    if (timet != null) rel.time = timet
  }
  def addUpConnection(up_join_keys : Array[Int], next_relation : Relation, related_qs:Array[QueryId], child_in_connex:Boolean, conn: TupleConnection) : Unit = {
    val ui = new UpwardInfo(rel.upward_infos.length, up_join_keys, next_relation,related_qs,child_in_connex,conn)

    if(conn!=null) conn.ui = ui
    rel.upward_infos.append(ui)

    var idx = rel.upward_infos.length - 1
    ui.Init(rel,idx)

  }

  def addConnection(keys : Array[Int], relation : Relation, p2c_qid: Array[(QueryId,QueryId)]) : TupleConnection = {
    if (rel.numChild > 0) {
      rel.conn_cnt = rel.conn_cnt + 1
      val joinKeyMapDescriptor = new MapStateDescriptor[Attributes, mutable.HashSet[Attributes]](rel.name+"joinKeyMap"+rel.conn_cnt, TypeInformation.of(classOf[Attributes]), TypeInformation.of(classOf[mutable.HashSet[Attributes]]))
      val aliveKeyMapDescriptor = new MapStateDescriptor[Attributes, mutable.HashSet[Attributes]](rel.name+"aliveKeyMap"+rel.conn_cnt, TypeInformation.of(classOf[Attributes]), TypeInformation.of(classOf[mutable.HashSet[Attributes]]))
      val globalKeyMapDescriptor = new MapStateDescriptor[Attributes, mutable.HashMap[QueryId,mutable.HashSet[Attributes]]](
        rel.name + "globalAliveKeyMap" + rel.conn_cnt,
        TypeInformation.of(classOf[Attributes]),
        TypeInformation.of(classOf[mutable.HashMap[QueryId,mutable.HashSet[Attributes]]])
      )
      val QMapDescriptor = new MapStateDescriptor[QueryId,QueryId](
              rel.name + "QMapDescriptor" + rel.conn_cnt,
              TypeInformation.of(classOf[QueryId]),
              TypeInformation.of(classOf[QueryId])
            )

      val joinKeyMap = rel.runtime.getMapState(joinKeyMapDescriptor)
      val aliveKeyMap = rel.runtime.getMapState(aliveKeyMapDescriptor)
      val globalAliveKeyMap = rel.runtime.getMapState(globalKeyMapDescriptor)
      val QMap = rel.runtime.getMapState(QMapDescriptor)

      rel.connection.append(new TupleConnection(relation, keys, joinKeyMap, aliveKeyMap,globalAliveKeyMap,QMap,null))

      p2c_qid.foreach { case (k, v) => rel.connection.last.p2c_qid.put(k, v) }

    } else throw new Exception("Add connection to non-child node")
    rel.connection.last
  }

}
