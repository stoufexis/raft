package com.stoufexis.raft.model

import cats.*

import com.stoufexis.raft.typeclass.IntLike

opaque type Term = Long

object Term:
  extension (n: Term)
    infix def long: Long = n

  inline def uninitiated: Term = 0
  inline def init:        Term = 1

  infix def apply(l: Long): Term = l

  inline def deriveFunctor[F[_]](using F: Functor[F], base: F[Long]): F[Term] =
    F.map(base)(identity)

  inline def deriveContravariant[F[_]](using F: Contravariant[F], base: F[Long]): F[Term] =
    F.contramap(base)(identity)

  given IntLike[Term]        = IntLike.IntLikeLong
  given CanEqual[Term, Term] = CanEqual.derived
