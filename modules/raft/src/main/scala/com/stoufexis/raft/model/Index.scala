package com.stoufexis.raft.model

import cats.*

import com.stoufexis.raft.typeclass.IntLike

opaque type Index = Long

object Index:
  inline def uninitiated: Index = 0
  inline def init:        Index = 1

  inline def apply(i: Long): Index = i

  extension (i: Index)
    infix def long: Long = i

  inline def deriveFunctor[F[_]](using F: Functor[F], base: F[Long]): F[Index] =
    F.map(base)(identity)

  inline def deriveContravariant[F[_]](using F: Contravariant[F], base: F[Long]): F[Index] =
    F.contramap(base)(identity)

  given IntLike[Index]         = IntLike.IntLikeLong
  given CanEqual[Index, Index] = CanEqual.derived
