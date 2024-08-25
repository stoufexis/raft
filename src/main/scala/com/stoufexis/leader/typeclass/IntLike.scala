package com.stoufexis.leader.typeclass

trait IntLike[A]:
  def zero: A

  def add(a: A, i: Int): A

  def toInt(a: A): Int

  val ord: Ordering[A]

object IntLike:
  extension [A](a: A)(using inc: IntLike[A])
    def toInt: Int = inc.toInt(a)

    infix def +(i: Int): A = inc.add(a, i)

    infix def -(i: Int): A = inc.add(a, -1 * i)

    infix def >(b: A): Boolean = inc.ord.gt(a, b)

    infix def <(b: A): Boolean = inc.ord.lt(a, b)

    infix def <=(b: A): Boolean = inc.ord.lteq(a, b)

  given IntLikeInt: IntLike[Int] with
    def zero: Int = 0

    def add(a: Int, i: Int): Int = a + i

    def toInt(a: Int): Int = a

    val ord: Ordering[Int] = summon