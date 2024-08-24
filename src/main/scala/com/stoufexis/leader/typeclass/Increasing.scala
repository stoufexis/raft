package com.stoufexis.leader.typeclass

trait Increasing[A]:
  def init: A

  def increaseBy(a: A, by: Int): A

  def toInt(a: A): Int

  val ord: Ordering[A]

object Increasing:
  extension [A](a: A)(using inc: Increasing[A])
    def next: A = inc.increaseBy(a, 1)

    def toInt: Int = inc.toInt(a)

    infix def >(b: A): Boolean = inc.ord.gt(a, b)

    infix def <(b: A): Boolean = inc.ord.lt(a, b)

    infix def <=(b: A): Boolean = inc.ord.lteq(a, b)

  given IncreasingInt: Increasing[Int] with
    def init: Int = 0

    def increaseBy(a: Int, by: Int): Int = a + by

    def toInt(a: Int): Int = a

    val ord: Ordering[Int] = summon
