package RelationType.Util

import RelationType.Relation
import RelationType.Attributes
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
import com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.Type

class QueryId() extends Serializable {
  var qid: Int = -1
  var place_id: Int = -1

  private val serialVersionUID: Long = 1L

  override def toString: String = s"QueryId(qid=$qid, place_id=$place_id)"

  override def equals(obj: Any): Boolean = obj match {
    case that: QueryId =>
      this.qid == that.qid && this.place_id == that.place_id
    case _ => false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + qid
    result = prime * result + place_id
    result
  }
}

class UpwardInfo(var idx: Int, var joinkey : Array[Int], var nextRelation : Relation, var related_q: Array[QueryId],var child_in_connex: Boolean, var conn: TupleConnection)
{

  var join_key_idx_in_output_attr : Array[Int] = Array(-1)
  var alive : MapState[Attributes, mutable.HashMap[Attributes,Int]] = _

  var onhold : MapState[Attributes, mutable.HashMap[Attributes, Int]] = _
  var keyCount : MapState[Attributes, Int] = _
  var on_alive_edit_path: Boolean = false

  def Init(rel: Relation, id: Int) : Unit ={
    if(child_in_connex)
    {
      join_key_idx_in_output_attr = joinkey.map(attr => rel.output_attr.indexOf(attr))

      for (i <- join_key_idx_in_output_attr) {
        if (i == -1) throw new Exception("Join key not found in output attr for relation " + rel.name)
      }
    }
    rel.up_conn_cnt +=1

    val aliveDescriptor = new MapStateDescriptor[Attributes, mutable.HashMap[Attributes,Int]](rel.name+"alive"+rel.up_conn_cnt, TypeInformation.of(classOf[Attributes]), TypeInformation.of(classOf[mutable.HashMap[Attributes, Int]]))
    val keyCountDescriptor = new MapStateDescriptor[Attributes, Int](rel.name+"keyCount"+rel.up_conn_cnt, TypeInformation.of(classOf[Attributes]), BasicTypeInfo.INT_TYPE_INFO.asInstanceOf[TypeInformation[Int]])

    alive = rel.runtime.getMapState(aliveDescriptor)
    keyCount = rel.runtime.getMapState(keyCountDescriptor)
    if (rel.numChild > 0) {
      val onholdDescriptor = new MapStateDescriptor[Attributes, mutable.HashMap[Attributes, Int]](rel.name+"onhold"+rel.up_conn_cnt, TypeInformation.of(classOf[Attributes]), TypeInformation.of(classOf[mutable.HashMap[Attributes, Int]]))
      onhold = rel.runtime.getMapState(onholdDescriptor)
    }
  }

}

class TupleConnection(var child_rel: Relation,
                      var parent_jk: Array[Int],
                      var k2onhold: MapState[Attributes, mutable.HashSet[Attributes]],
                      var k2alive: MapState[Attributes, mutable.HashSet[Attributes]],
                      var live_view: MapState[Attributes, mutable.HashMap[QueryId,mutable.HashSet[Attributes]]],
                      var p2c_qid: MapState[QueryId,QueryId],
                      var ui: UpwardInfo
                     )
{

}
