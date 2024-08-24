package com.stoufexis.leader.typeclass

trait Counter[A]:
  def init: A

  def increaseBy(a: A, by: Int): A

  def decreaseBy(a: A, by: Int): A

  def toInt(a: A): Int

  val ord: Ordering[A]

object Counter:
  extension [A](a: A)(using inc: Counter[A])
    def next: A = inc.increaseBy(a, 1)

    def previous: A = inc.decreaseBy(a, 1)

    def increaseBy(by: Int): A = inc.increaseBy(a, by)

    def decreaseBy(by: Int): A = inc.decreaseBy(a, by)

    def toInt: Int = inc.toInt(a)

    infix def >(b: A): Boolean = inc.ord.gt(a, b)

    infix def <(b: A): Boolean = inc.ord.lt(a, b)

    infix def <=(b: A): Boolean = inc.ord.lteq(a, b)

  given IncreasingInt: Counter[Int] with
    def init: Int = 0

    def increaseBy(a: Int, by: Int): Int = a + by

    def decreaseBy(a: Int, by: Int): Int = a - by

    def toInt(a: Int): Int = a

    val ord: Ordering[Int] = summon
