package RelationType

import scala.collection.mutable.ArrayBuffer

case class Payload(var _1: String,
                   var _2: String,
                   var _3: Any,
                   var _4: Attributes,
                   var _5: Long) extends Serializable {

  private var primaryKey: Array[Any] = null

  def this(timeStamp: Long,  valueArray: Array[Any]) = {
    this("Tuple", "Tuple", 0, Attributes(valueArray), timeStamp)
  }

  override def equals(obj: Any): Boolean = {
    if (obj.getClass == this.getClass) {
      if (obj.asInstanceOf[Payload]._4 == this._4) {
        true
      } else
        false
    } else {
      false
    }
  }

  override def hashCode(): Int = {
    _4.hashCode()
  }

}
