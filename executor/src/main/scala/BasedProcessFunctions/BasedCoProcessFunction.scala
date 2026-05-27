package BasedProcessFunctions

import org.apache.flink.api.common.state.ValueState
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector

abstract class BasedCoProcessFunction[K, I, O](windowLength: Long,
                                               slidingSize: Long,
                                               name: String,
                                               testMemory: Boolean = false) extends KeyedCoProcessFunction[K, I, I, O] {

  val Tor = 1e-6

  val prefix = "Co Process Function"

  var startTime: Long = Long.MaxValue

  var duration: Long = 0L

  var CurrentMaximumTimeStamp: ValueState[Long] = _

  var LatestExpireEle: ValueState[Long] = _

  var NextOutput: ValueState[Long] = _

  var CurrentOutput: Long = _

  var OutputAccur: Long = 0L

  var StoreAccur: Long = 0L

  var LatestExpired: Long = 0L

  var count = 0

  def initstate(): Unit

  def enumeration(out: Collector[O]): Unit

  def expire(ctx: KeyedCoProcessFunction[K, I, I, O]#Context): Unit

  def process(value_raw: I, ctx: KeyedCoProcessFunction[K, I, I, O]#Context, out: Collector[O]): Unit

  def storeStream(value: I, ctx: KeyedCoProcessFunction[K, I, I, O]#Context): Unit

  def testExists(value: I, ctx: KeyedCoProcessFunction[K, I, I, O]#Context): Boolean

  override def open(parameters: Configuration): Unit = {
    initstate()

    if (testMemory) {
      Runtime.getRuntime.runFinalization()
      Runtime.getRuntime.gc()
      Thread.sleep(30000)
      System.out.println(s"Process Function $name : Memory usage ${(Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()) / (1024 * 1024)}, EnumerationTime $OutputAccur, StorageTime $StoreAccur")
    }

  }

  override def processElement1(value: I, ctx: KeyedCoProcessFunction[K, I, I, O]#Context, out: Collector[O]): Unit = {
    val s = System.currentTimeMillis()
    if (startTime > System.currentTimeMillis()) startTime = System.currentTimeMillis()

    processBuffer(value, ctx, out)
    process(value, ctx, out)
    duration += System.currentTimeMillis() - s

  }

  def processBuffer(value: I, ctx: KeyedCoProcessFunction[K, I, I, O]#Context, out: Collector[O]): Unit = {}

  override def processElement2(value: I, ctx: KeyedCoProcessFunction[K, I, I, O]#Context, out: Collector[O]): Unit = {
    val s = System.currentTimeMillis()
    if (startTime > System.currentTimeMillis()) startTime = System.currentTimeMillis()

    processBuffer(value, ctx, out)
    process(value, ctx, out)
    duration += System.currentTimeMillis() - s
  }

  override def close(): Unit = {
    val endTime = System.currentTimeMillis()
    println(s"$prefix $name Parallelism ${getRuntimeContext.getIndexOfThisSubtask} StartTime $startTime EndTime $endTime Difference ${endTime - startTime} AccumulateTime $duration EnumerationTime $OutputAccur, StorageTime $StoreAccur")
  }
}
