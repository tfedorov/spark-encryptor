package com.tfedorov.blotto

case class PlayerStep(field1: Int, field2: Int, field3: Int, field4: Int) {
  def sum: Int = field1 + field2 + field3 + field4

  def compare(another: PlayerStep): Int = {
    field1.compareTo(another.field1) +
      field2.compareTo(another.field2) +
      field3.compareTo(another.field3) +
      field4.compareTo(another.field4)
  }
}

object PlayerStep {

  def generateAll(max: Int): Seq[PlayerStep] = {
    val possibles: Seq[Int] = 1 to (max - 2)
    for (f1 <- possibles;
         f2 <- possibles;
         f3 <- possibles;
         f4 <- possibles)
      yield PlayerStep(f1, f2, f3, f4)
  }
}
