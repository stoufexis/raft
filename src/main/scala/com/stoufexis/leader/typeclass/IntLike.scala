package com.stoufexis.leader.typeclass

trait IntLike[A]:
  def zero: A

  def add(a: A, i: Int): A

  def toInt(a: A): Int

object IntLike:
  extension [A](a: A)(using inc: IntLike[A])
    def toInt: Int = inc.toInt(a)

    infix def +(i: Int): A = inc.add(a, i)

    infix def -(i: Int): A = inc.add(a, -1 * i)

    infix def >(b: A): Boolean = IntLikeOrdering[A].gt(a, b)

    infix def <(b: A): Boolean = IntLikeOrdering[A].lt(a, b)

    infix def <=(b: A): Boolean = IntLikeOrdering[A].lteq(a, b)

  given IntLikeInt: IntLike[Int] with
    def zero: Int = 0

    def add(a: Int, i: Int): Int = a + i

    def toInt(a: Int): Int = a

  given IntLikeOrdering[A](using ia: IntLike[A]): Ordering[A] =
    Ordering.fromLessThan((x, y) => ia.toInt(x) < ia.toInt(y))