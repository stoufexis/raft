package com.stoufexis.leader

import com.stoufexis.leader.MySet.cond

trait MySet[A](using CanEqual[A, A]):

  def contains(a: A): Boolean

  def diff(that: MySet[A]): MySet[A] =
    cond(a => contains(a) && !that.contains(a))

  def intersection(that: MySet[A]): MySet[A] =
    cond(a => contains(a) && that.contains(a))

  def inverse: MySet[A] =
    cond(!contains(_))

  def remove(a: A): MySet[A] =
    cond(a2 => a2 != a && contains(a))

  def add(a: A): MySet[A] =
    cond(a2 => a2 == a || contains(a))

  def union(that: MySet[A]): MySet[A] =
    cond(a2 => contains(a2) || that.contains(a2))

object MySet:
  def apply[A](elems: A*)(using CanEqual[A, A]): MySet[A] = new:
    def contains(a: A): Boolean =
      elems.exists(_ == a)

  def cond[A](f: A => Boolean)(using CanEqual[A, A]): MySet[A] = new:
    def contains(a: A): Boolean = f(a)
