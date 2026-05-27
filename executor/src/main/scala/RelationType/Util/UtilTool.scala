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

class UtilTool(rel:Relation) {
  def FindParentRel(query_id: QueryId): Relation = {
    var upward_info: UpwardInfo = null
    for(i<- rel.upward_infos)
    {
      if(i.related_q.contains(query_id))
      {
        return i.nextRelation
      }
    }
    null
  }
  def FindParentRelWithPId(query_id: QueryId): (Relation,Int,TupleConnection) = {
    var upward_info: UpwardInfo = null
    for(i<- rel.upward_infos)
    {
      if(i.related_q.contains(query_id))
      {
        val parent = i.nextRelation
        if(parent == null)
        {
          return null
        }
        val conn = i.conn
        var parent_pid:Int = -1
        for(kv<- conn.p2c_qid.entries().asScala)
        {
          if(kv.getValue() == query_id)
          {
            parent_pid = kv.getKey().place_id
          }
        }
        assert(parent_pid != -1)

        return (parent, parent_pid,conn)
      }
    }
    null
  }

  def FindUpwardInfo(query_id: QueryId): UpwardInfo = {

    var upward_info: UpwardInfo = null
    for(i<- rel.upward_infos)
    {

      if(i.related_q.contains(query_id))
      {
        return i
      }
    }
    println("Error: cannot find upward info for query id "+query_id.toString+" in relation "+rel.name)
    assert(false)
    null
  }
  def GetOutputAttr(query_id:QueryId):Array[Int]=
  {
    if(FindUpwardInfo(query_id).child_in_connex == true)
    {
      return rel.output_attr
    }
    else
    {
      return Array()
    }
  }

  def GetUnion(qs: Array[QueryId], conn: TupleConnection, keys: Attributes):mutable.HashMap[Attributes,Array[QueryId]]=
  {
    var tuples: mutable.HashMap[Attributes,Array[QueryId]] = mutable.HashMap()
    for(q<- qs)
    {
      assert(conn != null)
      assert(conn.live_view != null)
      if(conn.live_view.get(keys)==null)
      {
        println("Error: in relation "+rel.name+" for query id "+q.toString+", cannot find keys "+keys.toString+" in tuple connection index. child: "+conn.child_rel.name)
        assert(false)
      }

      var tuple_set: mutable.HashSet[Attributes] = conn.live_view.get(keys).getOrElse(q, mutable.HashSet[Attributes]())
      for(t<-tuple_set)
      {
        if(!tuples.contains(t))
        {
          tuples.put(t, Array(q))
        }
        else
        {
          val existing_qs = tuples.get(t).get
          tuples.put(t, existing_qs :+ q)
        }
      }
    }

    if(rel.glob_info.isTargetLine) {
      rel.glob_info.debugContext("GetUnion", s" rel=${rel.name} keys=$keys conn=${conn.child_rel.name} qs=[${qs.filter(_!=null).mkString(",")}] result=${tuples.map{case(k,v) => s"$k->[${v.mkString(",")}]"}.mkString("; ")}")
    }
    tuples
  }

  def AssertInConnex(qid: QueryId):Unit=
  {
    val ui = FindUpwardInfo(qid)
    assert(ui.child_in_connex)
  }

  def QidTransform(parent_q: QueryId, r: TupleConnection):QueryId=
  {
    r.p2c_qid.get(parent_q)
  }

  def QidTransform(parent_qs: Array[QueryId], r: TupleConnection):Array[QueryId]=
  {

    var ans_buf = ArrayBuffer[QueryId]()
    for(q<- parent_qs)
    {

      assert(r.p2c_qid.contains(q))
      ans_buf.append(r.p2c_qid.get(q))
    }
    ans_buf.toArray
  }

  def QidTransform(parent_qs: Array[QueryId], ui: UpwardInfo):Array[QueryId]=
  {
    QidTransform(parent_qs, ui.conn)
  }

}
