import BasedProcessFunctions.BasedProcessFunction
import RelationType.{Attributes, Payload, Relation,GeneralizedRelation}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._
import scala.collection.mutable
import RelationType.Util.GlobalInfo

class GenericProcessFunction(val deltaEnumMode: Int, queryPlan: QueryPlan,output_schema: Array[Array[(String,Int, Int)]], val outputPath: String = "out.txt", val timeoutMs: Long = 18 * 3600 * 1000L)
  extends BasedProcessFunction[Any, Payload, String](0, 0, "GenericProcessor") {

  private val relations = mutable.Map[String, Relation]()
  private var outSet = false
  private var num_updates:Int = 0
  private var global_info: GlobalInfo = _
  private var startTimeMs: Long = _

  override def initstate(): Unit = {
    println("start init state")
    startTimeMs = System.currentTimeMillis()

    global_info = new GlobalInfo()
    global_info.outputFilePath = outputPath

    queryPlan.relations.foreach { config =>

      val relation = config.relationType match {
        case "leaf" => new leafRelation(config.name, deltaEnumMode, config.output_attr,global_info)
        case "middle" => new middleRelation(config.name, deltaEnumMode,config.numChildren, config.output_attr,global_info)
        case "generalized" => new GeneralizedRelation(config.name, deltaEnumMode, config.numChildren,  config.output_attr,global_info) {}
        case "dummy_root" => new middleRelation(config.name, deltaEnumMode,config.numChildren, config.output_attr,global_info)
      }
      relations(config.name) = relation
    }

    var output_array_buf: mutable.ArrayBuffer[(RelationType.Relation, Int, Int)] = mutable.ArrayBuffer()
    var big_buf: mutable.ArrayBuffer[Array[(RelationType.Relation,Int, Int)]] = mutable.ArrayBuffer()
    for(out_sch<-output_schema)
    {
      out_sch.foreach { case (rel_name, place_id, attr_idx) =>
        val rel = relations.getOrElse(rel_name, throw new NoSuchElementException(s"Relation $rel_name not found"))
        output_array_buf.append((rel, place_id, attr_idx))
      }
      big_buf.append(output_array_buf.toArray)
      output_array_buf.clear()
    }

    global_info.output_result_schema = big_buf.toArray
    global_info.PrintOutputSchema()

    relations.values.foreach { relation =>
      relation.init_tool.initState(getRuntimeContext)
    }

    queryPlan.connections.foreach { conn =>
      {
        if(conn.fromRelation == "dummy_root")
        {
          relations(conn.toRelation).init_tool.addUpConnection(
            conn.child_upward_join_keys,
            null,
            conn.related_qs,
            conn.child_in_connex,
            null
          )
        }
        else
        {
          val tup_conn = relations(conn.fromRelation).init_tool.addConnection(
            conn.joinKeys,
            relations(conn.toRelation),
            conn.parent2child_qids
          )
          relations(conn.toRelation).init_tool.addUpConnection(
            conn.child_upward_join_keys,
            relations(conn.fromRelation),
            conn.related_qs,
            conn.child_in_connex,
            tup_conn
          )
        }

      }
    }

    relations.values.foreach { relation =>
      relation.init_tool.InitOutputProjectInfo()
    }

    println("Initialized relations and connections.")

  }

  override def enumeration(out: Collector[String]): Unit = ???

  override def expire(ctx: KeyedProcessFunction[Any, Payload, String]#Context): Unit = ???

  override def process(
    value: Payload,
    ctx: KeyedProcessFunction[Any, Payload, String]#Context,
    out: Collector[String]
  ): Unit = {

    if (!outSet) {
      outSet = true
      relations.values.foreach(_.init_tool.setOut(out))
    }

    global_info.currentFileLine = value._5
    global_info.currentOperation = value._1
    global_info.currentRelationName = value._2
    global_info.currentTuple = value._4

    val elapsed = System.currentTimeMillis() - startTimeMs
    if (elapsed > timeoutMs) {
      val elapsedHours = elapsed / 3600000.0
      val timeoutHours = timeoutMs / 3600000.0
      println(f"[TIMEOUT] Elapsed $elapsedHours%.2fh > limit $timeoutHours%.2fh at line=${value._5} action=${value._1} rel=${value._2}. Exiting.")
      sys.exit(1)
    }

    global_info.debugTargetLine = -1L
    global_info.debugTargetValue = ""

    relations.get(value._2) match {
      case Some(relation) => value._1 match {
        case "Insert" => relation.insert(value._4)
        case "Delete" => relation.delete(value._4)

        case _ =>
      }
      case None =>
    }
    num_updates += 1

  }

  override def storeStream(value: Payload, ctx: KeyedProcessFunction[Any, Payload, String]#Context): Unit = ???

  override def testExists(value: Payload, ctx: KeyedProcessFunction[Any, Payload, String]#Context): Boolean = ???

  override def close(): Unit = {

    super.close()
  }

  class leafRelation(name : String, override val deltaEnumMode: Int,  output_attr: Array[Int],glob_info: GlobalInfo) extends Relation(name, deltaEnumMode, 0, output_attr,glob_info) {

    def enumerate(t : Attributes) : Unit = {
      throw new NoSuchMethodError("this function not edited ")
    }

  }

  class middleRelation(name : String, override val deltaEnumMode: Int, numChildren: Int,  output_attr:Array[Int],glob_info:GlobalInfo) extends Relation(name, deltaEnumMode, numChildren,output_attr,glob_info) {

    def enumerate(t : Attributes) : Unit = {
      throw new NoSuchMethodError("this function not edited ")

    }

    def enumerateD(t : Attributes) : Unit = {
      throw new NoSuchMethodError("this function not edited ")

    }

  }
}
