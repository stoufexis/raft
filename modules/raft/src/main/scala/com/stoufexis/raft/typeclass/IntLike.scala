package com.stoufexis.raft.typeclass

trait IntLike[A]:
  def add(a: A, i: Int): A

  def toLong(a: A): Long

object IntLike:
  extension [A](a: A)(using inc: IntLike[A])
    def toLong: Long = inc.toLong(a)

    infix def +(i: Int): A = inc.add(a, i)

    infix def -(i: Int): A = inc.add(a, -1 * i)

    infix def >(b: A): Boolean = IntLikeOrdering[A].gt(a, b)

    infix def <(b: A): Boolean = IntLikeOrdering[A].lt(a, b)

    infix def <=(b: A): Boolean = IntLikeOrdering[A].lteq(a, b)

    infix def >=(b: A): Boolean = IntLikeOrdering[A].gteq(a, b)
    
    infix def diff(b: A): Long = a.toLong - b.toLong

  given IntLikeInt: IntLike[Int] with
    def add(a: Int, i: Int): Int = a + i

    def toLong(a: Int): Long = a

  given IntLikeLong: IntLike[Long] with
    def add(a: Long, i: Int): Long = a + i

    def toLong(a: Long): Long = a

  given IntLikeOrdering[A](using ia: IntLike[A]): Ordering[A] =
    Ordering.fromLessThan((x, y) => ia.toLong(x) < ia.toLong(y))
