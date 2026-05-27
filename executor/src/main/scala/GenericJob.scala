import RelationType.{Attributes, Payload}
import RelationType.Util.QueryId
import Util.KeyListMap
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.DiscardingSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

case class QueryPlan(
  relations: List[RelationConfig],
  connections: List[ConnectionConfig]
)

case class RelationConfig(
  name: String,
  relationType: String,
  numChildren: Int,
  output_attr: Array[Int]
)

case class ConnectionConfig(
  fromRelation: String,
  toRelation: String,
  joinKeys: Array[Int],
  child_upward_join_keys: Array[Int],
  related_qs: Array[QueryId],
  child_in_connex: Boolean,
  parent2child_qids: Array[(QueryId, QueryId)]
)

object QueryPlanParser {
  private val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def parse(planJson: String): QueryPlan = {
    objectMapper.readValue(planJson, classOf[QueryPlan])
  }
}

object GenericJob {
  var nFilter: Long = -1

  def ReadInputSchema(lines: List[String]):Map[String, List[(String, Int, Array[Int])]]=
  {
    lines.map { line =>
      val tokens = line.split(",").map(_.trim)
      require(tokens.length >= 4, s"Invalid schema line: '$line' - at least 4 values required")

      val eventIndicator = tokens(0)
      val relationName = tokens(1)
      val keyIndex = tokens(2).toInt
      val attributeIndices = tokens.drop(3).map(_.toInt)

      (eventIndicator, (relationName, keyIndex, attributeIndices))
    }
    .groupBy(_._1)
    .map { case (indicator, tuples) =>
      (indicator, tuples.map(_._2))
    }
  }
  def ReadOutputSchema(lines: List[String]):Array[(String, Int, Int)]=
  {
    lines.map { line =>
      val tokens = line.split(",").map(_.trim)
      require(tokens.length == 2, s"Invalid output schema line: '$line' - exactly 2 values required")

      val (relationName, placeId) = tokens(0) match {
        case s if s.contains(".") =>
          val parts = s.split("\\.", 2)
          (parts(0), parts(1).toInt)
        case s => (s, 0)
      }

      val index = tokens(1).toInt
      (relationName, placeId, index)
    }.toArray
  }

  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)
    val path = params.get("path", "")
    val graphName = params.get("graph", "ms_snb_output.csv")
    val parallelism = params.get("parallelism", "1").toInt
    val planFile = params.get("planFile", "")

    val planJson = if (planFile.nonEmpty) {
      try {
        scala.io.Source.fromFile(planFile).getLines().mkString
      } catch {
        case e: Exception =>
          println(s"error: ${e.getMessage}")
          ""
      }
    } else {
       println("planFile is empty!")
       ""
    }
    val schemaFile = params.get("schemaFile", "")
    var output_result_schema: Array[Array[(String, Int, Int)]] = Array.empty

    val schema: Map[String, List[(String, Int, Array[Int])]] =
    {
      if (schemaFile.isEmpty) Map.empty
      else {
        try {
          val lines = scala.io.Source.fromFile(schemaFile).getLines().filterNot(_.trim.isEmpty).toList

          val (eventLines, outputLines) = lines.partition { line =>
            val tokens = line.split(",").map(_.trim)
            tokens.length >= 4
          }

          val eventSchema = ReadInputSchema(eventLines)

          val queryBlocks = outputLines.tail.foldLeft(List[List[String]]()) { (acc, line) =>
            if (line.matches("\\d+")) {

              List() :: acc
            }
            else {

              if (acc.isEmpty) List(List(line)) else (line :: acc.head) :: acc.tail
            }
          }.reverse.map(_.reverse)

          output_result_schema = queryBlocks.map(block => ReadOutputSchema(block)).toArray

          eventSchema
        } catch {
          case e: Exception =>
            println(s"Failed to read schema file: ${e.getMessage}")
            Map.empty
        }
      }
    }

    val TWO_DAYS_MS = 172800000L
    val TWO_DAYS_S = "172800 s"
    val TWO_DAYS_H = "48 h"

    System.setProperty("heartbeat.timeout", TWO_DAYS_MS.toString)
    System.setProperty("heartbeat.interval", TWO_DAYS_MS.toString)
    System.setProperty("akka.heartbeat.interval", TWO_DAYS_S)
    System.setProperty("akka.heartbeat.pause", TWO_DAYS_S)
    System.setProperty("akka.cluster.failure-detector.acceptable-heartbeat-pause", TWO_DAYS_S)
    System.setProperty("akka.cluster.failure-detector.threshold", "1000.0")
    System.setProperty("akka.ask.timeout", TWO_DAYS_H)
    System.setProperty("akka.tcp.timeout", TWO_DAYS_H)
    System.setProperty("akka.lookup-timeout", TWO_DAYS_H)
    System.setProperty("akka.client.timeout", TWO_DAYS_H)
    System.setProperty("akka.cluster.operation-timeout", TWO_DAYS_H)
    System.setProperty("flink.rpc.timeout", TWO_DAYS_H)
    System.setProperty("taskmanager.network.netty.server.idleTimeout", TWO_DAYS_S)
    System.setProperty("taskmanager.network.netty.client.idleTimeout", TWO_DAYS_S)
    System.setProperty("task.cancellation.timeout", "0")
    System.setProperty("execution.checkpointing.timeout", TWO_DAYS_MS.toString)
    System.setProperty("network.memory.request-timeout", TWO_DAYS_MS.toString)

    val config = new Configuration()

    config.setString("taskmanager.memory.network.min", s"${math.max(128, 64 * parallelism)}Mb")
    config.setString("taskmanager.memory.network.max", s"${math.max(128, 64 * parallelism)}Mb")

    config.setLong("heartbeat.timeout", TWO_DAYS_MS)
    config.setLong("heartbeat.interval", TWO_DAYS_MS)

    config.setString("akka.heartbeat.interval", TWO_DAYS_S)
    config.setString("akka.heartbeat.pause", TWO_DAYS_S)
    config.setString("akka.cluster.failure-detector.acceptable-heartbeat-pause", TWO_DAYS_S)
    config.setString("akka.cluster.failure-detector.threshold", "1000.0")

    config.setString("akka.ask.timeout", TWO_DAYS_H)
    config.setString("akka.tcp.timeout", TWO_DAYS_H)
    config.setString("akka.lookup-timeout", TWO_DAYS_H)
    config.setString("akka.client.timeout", TWO_DAYS_H)
    config.setString("akka.cluster.operation-timeout", TWO_DAYS_H)

    config.setString("flink.rpc.timeout", TWO_DAYS_H)

    config.setString("taskmanager.network.netty.server.idleTimeout", TWO_DAYS_S)
    config.setString("taskmanager.network.netty.client.idleTimeout", TWO_DAYS_S)

    config.setLong("task.cancellation.timeout", 0L)
    config.setLong("execution.checkpointing.timeout", TWO_DAYS_MS)
    config.setLong("network.memory.request-timeout", TWO_DAYS_MS)

    org.apache.flink.configuration.GlobalConfiguration.loadConfiguration(config)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.configure(config, getClass.getClassLoader)
    env.setParallelism(parallelism)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.enableObjectReuse()

    nFilter = params.get("n", "-1").toLong
    val fullEnumEnable = params.get("fullEnumEnable", "false") == "true"
    val deltaEnumEnable = params.get("deltaEnumEnable", "false") == "true"
    val dataFormat = params.get("dataFormat", "snb")
    val outputFilePath = params.get("outputFile", "out.txt")
    val deltaEnumMode = 1

    val inputStream = getStream(env, s"$path/$graphName", fullEnumEnable, schema, dataFormat)
    val queryPlan = QueryPlanParser.parse(planJson)

    val timeoutMs = dataFormat match {
      case "job" => 8 * 3600 * 1000L
      case _     => 35 * 3600 * 1000L
    }

    val result = inputStream.keyBy(i=>i._3)
      .process(new GenericProcessFunction(deltaEnumMode, queryPlan, output_result_schema, outputFilePath, timeoutMs))

    result.addSink(new DiscardingSink[String])
    env.execute("Generic Query Processor")
  }

  private def getStream(env: StreamExecutionEnvironment,
                      dataPath: String,
                      fullEnumEnable: Boolean,
                      schema: Map[String, List[(String, Int, Array[Int])]],
                      dataFormat: String): DataStream[Payload] = {

    val data = env.readTextFile(dataPath).setParallelism(1)
    val parallel: Int = env.getParallelism
    var cnt: Long = 0

    def splitCSVFields(s: String): Array[String] = {
      if (s == null || s.isEmpty) return Array.empty[String]
      val result = scala.collection.mutable.ArrayBuffer[String]()
      val sb = new StringBuilder()
      var inQuote = false
      var i = 0
      while (i < s.length) {
        val c = s.charAt(i)
        if (c == '\\' && inQuote && i + 1 < s.length) {
          val next = s.charAt(i + 1)
          if (next == '"') {

            sb.append('"')
          } else if (next == '\\') {

            sb.append('\\')
          } else {

            sb.append(c)
            sb.append(s.charAt(i + 1))
          }
          i += 2
        } else if (c == '"') {
          inQuote = !inQuote
          i += 1
        } else if (c == ',' && !inQuote) {
          result += sb.toString.trim
          sb.clear()
          i += 1
        } else {
          sb.append(c)
          i += 1
        }
      }
      result += sb.toString.trim
      result.toArray
    }

    val T: DataStream[Payload] = data
      .process(new ProcessFunction[String, Payload] {
        override def processElement(
            value: String,
            ctx: ProcessFunction[String, Payload]#Context,
            out: Collector[Payload]): Unit = {

          if (dataFormat == "job") {

            val trimmed = value.trim
            val firstSpace = trimmed.indexOf(' ')
            if (firstSpace < 0) return

            val firstToken = trimmed.substring(0, firstSpace).trim
            val rest = trimmed.substring(firstSpace + 1).trim
            val secondSpace = rest.indexOf(' ')

            if (firstToken == "1" || firstToken == "0") {

              if (secondSpace < 0) return
              val tableName = rest.substring(0, secondSpace).trim
              val fieldsStr = rest.substring(secondSpace + 1).trim
              val action = if (firstToken == "1") "Insert" else "Delete"
              val cells = splitCSVFields(fieldsStr).map(s => if (s.isEmpty) "null" else s)

              cnt += 1
              schema.get(tableName) match {
                case Some(relationConfigs) =>
                  relationConfigs.foreach { case (relName, keyIndex, attrIndices) =>
                    if (keyIndex >= 0 && keyIndex < cells.length) {
                      val maxIdx = attrIndices.filter(_ >= 0).max
                      if (maxIdx >= cells.length) {
                        println(s"ERROR: line=$cnt table=$tableName rel=$relName cells.length=${cells.length} maxAttrIdx=$maxIdx data=${fieldsStr.take(100)}")
                        sys.exit(1)
                      }
                      val keyValue = cells(keyIndex).toLong
                      val partitionKey = keyValue % parallel
                      val attributeValues = attrIndices.map(idx => cells(idx)).toArray[Any]
                      out.collect(Payload(action, relName, partitionKey, Attributes(attributeValues), cnt))
                    }
                  }
                case None =>
              }
            } else {

              val tableName = firstToken
              val fieldsStr = rest.trim
              val cells = splitCSVFields(fieldsStr).map(s => if (s.isEmpty) "null" else s)

              cnt += 1
              schema.get(tableName) match {
                case Some(relationConfigs) =>
                  relationConfigs.foreach { case (relName, keyIndex, attrIndices) =>
                    if (keyIndex >= 0 && keyIndex < cells.length) {
                      val maxIdx = attrIndices.filter(_ >= 0).max
                      if (maxIdx >= cells.length) {
                        println(s"ERROR: line=$cnt table=$tableName rel=$relName cells.length=${cells.length} maxAttrIdx=$maxIdx data=${fieldsStr.take(100)}")
                        sys.exit(1)
                      }
                      val keyValue = cells(keyIndex).toLong
                      val partitionKey = keyValue % parallel
                      val attributeValues = attrIndices.map(idx => cells(idx)).toArray[Any]
                      out.collect(Payload("Insert", relName, partitionKey, Attributes(attributeValues), cnt))
                    }
                  }
                case None =>
              }
            }
          } else {

            val strings = value.split("\\|")
            if (strings.length < 2) return

            val op = strings(0)
            val indicator = strings(1)
            val cells = strings.slice(2, strings.length)

            op match {
              case "+" | "-" =>
                cnt += 1
                val action = if (op == "+") "Insert" else "Delete"

                schema.get(indicator) match {
                  case Some(relationConfigs) =>
                    relationConfigs.foreach { case (relName, keyIndex, attrIndices) =>

                      if (keyIndex >= 0 && keyIndex < cells.length) {
                        val keyValue = cells(keyIndex).toLong
                        val partitionKey = keyValue % parallel

                        val attributeValues = attrIndices.map { idx =>
                          if (idx >= 0 && idx < cells.length) cells(idx) else null
                        }.toArray[Any]

                        out.collect(Payload(
                          action,
                          relName,
                          partitionKey,
                          Attributes(attributeValues),
                          cnt
                        ))
                      } else {

                      }
                    }
                  case None =>

                }

              case "*" if fullEnumEnable =>
                cnt += 1
                println(s"Trigger full enum at cnt = $cnt")
                for (i <- 0 until parallel) {
                  out.collect(Payload(
                    "Enumerate",
                    "",
                    i,
                    Attributes(Array.empty),
                    cnt
                  ))
                }

              case _ =>
            }
          }
        }
      }).setParallelism(1)

    T
  }
}
