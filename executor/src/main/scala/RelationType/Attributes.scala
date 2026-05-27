package RelationType

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

case class Attributes(values : Array[Any], basedannotation : Double = 1.0) extends Serializable {
  var annotation : Double = basedannotation

  var hashcodeBuffer : Int = 0

  override def clone(): Attributes = {

    val clonedValues = values.clone()

    new Attributes(clonedValues)

  }

  def projection(p_keys : Array[Int]) : Attributes = {
    var tempArray : ArrayBuffer[Any] = new ArrayBuffer()

    for (i <- p_keys) {
      tempArray.append(values(i))

    }
    new Attributes(tempArray.toArray,  annotation)
  }

  def join(that : Attributes) : Attributes = {

    val newValues = this.values ++ that.values

    val newAnnotation = this.annotation * that.annotation

    val result = Attributes(newValues, newAnnotation)
    result.annotation = newAnnotation
    result

  }

  override def equals(obj: Any): Boolean = {
    obj match {

      case attr: Attributes =>
        attr.values.sameElements(this.values)
      case _ =>
        false
    }
  }

  override def hashCode(): Int = {
    if (hashcodeBuffer == 0) {
      var result = 0
      for (i <- values.indices) {
        result = result ^ computeHashCode( values(i))
      }
      hashcodeBuffer = result
    }
    hashcodeBuffer
  }

  override def toString: String = {
    values.mkString(" , ")
  }

  def equalDouble(v1 : Double, v2 : Double) : Boolean = {
    (v1 - v2) * (v1 - v2) < 1e-6
  }

  def computeHashCode(fieldValue: Any): Int = {
    val valueHashCode = fieldValue match {
      case i: Int => i
      case s: String => s.##
      case l: Long => (l ^ 0xFFFFFFFF).toInt ^ (l >> 32).toInt
      case _ => throw new UnsupportedOperationException(s"please implement your hashcode function for ${fieldValue.getClass}")
    }

    valueHashCode
  }
}
