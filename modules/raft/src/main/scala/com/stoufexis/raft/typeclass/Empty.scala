package com.stoufexis.raft.typeclass

import scala.compiletime.*
import scala.deriving.Mirror
import cats.kernel.Monoid

trait Empty[A]:
  def empty: A

object Empty:
  def apply[A: Empty]: Empty[A] = summon

  def const[A](a: A): Empty[A] = new:
    def empty: A = a

  inline def derived[A <: Product](using m: Mirror.ProductOf[A]): Empty[A] =
    new:
      def empty: A = m.fromTuple(getAll[m.MirroredElemTypes])

  inline def getAll[T <: Tuple]: T =
    inline erasedValue[T] match
      case _: EmptyTuple =>
        val ev = summonInline[EmptyTuple =:= T]
        ev(EmptyTuple)

      case _: (h *: t) =>
        val ev = summonInline[(h *: t) =:= T]
        ev(summonInline[Empty[h]].empty *: getAll[t])

  given Empty[Int]    = const(0)
  given Empty[String] = const("")
  given Empty[Long]   = const(0)
  given Empty[Double] = const(0)

  given [S](using m: Monoid[S]): Empty[S] = const(m.empty)
