package BasedProcessFunctions

import org.apache.flink.api.common.state.ValueState
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

abstract class BasedProcessFunction[K, I, O](windowLength: Long, slidingSize: Long, name: String, testMemory: Boolean = false) extends KeyedProcessFunction[K, I, O] {

  val Tor = 1e-6

  val prefix = "Process Function"

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
  var inited = false

  def initstate(): Unit

  def enumeration(out: Collector[O]): Unit

  def expire(ctx: KeyedProcessFunction[K, I, O]#Context): Unit

  def process(value_raw: I, ctx: KeyedProcessFunction[K, I, O]#Context, out: Collector[O]): Unit

  def storeStream(value: I, ctx: KeyedProcessFunction[K, I, O]#Context): Unit

  def testExists(value: I, ctx: KeyedProcessFunction[K, I, O]#Context): Boolean

  override def open(parameters: Configuration): Unit = {

    if (testMemory) {
      assert(false)
      Runtime.getRuntime.runFinalization()
      Runtime.getRuntime.gc()
      Thread.sleep(30000)
      System.out.println(s"Process Function $name : Memory usage ${(Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()) / (1024 * 1024)}, EnumerationTime $OutputAccur, StorageTime $StoreAccur")
    }

  }

  override def processElement(value: I, ctx: KeyedProcessFunction[K, I, O]#Context, out: Collector[O]): Unit = {
    if(!inited)
    {
      initstate()
      inited = true
    }
    val s = System.nanoTime()
    if (startTime > System.nanoTime()) startTime = System.nanoTime()

    processBuffer(value, ctx, out)
    process(value, ctx, out)
    duration += System.nanoTime() - s
  }

  def processBuffer(value: I, ctx: KeyedProcessFunction[K, I, O]#Context, out: Collector[O]): Unit = {}

  override def close(): Unit = {
    val endTime = System.nanoTime()
    println(s"$prefix $name Parallelism ${getRuntimeContext.getIndexOfThisSubtask} StartTime $startTime EndTime $endTime Difference ${endTime - startTime} AccumulateTime $duration EnumerationTime $OutputAccur, StorageTime $StoreAccur")
  }
}
