package com.stoufexis.raft.model

import cats.*

opaque type NodeId = String

object NodeId:
  extension (n: NodeId)
    infix def string: String = n

  inline def apply(str: String): NodeId = str

  inline def deriveFunctor[F[_]](using F: Functor[F], base: F[String]): F[NodeId] =
    F.map(base)(identity)

  inline def deriveContravariant[F[_]](using F: Contravariant[F], base: F[String]): F[NodeId] =
    F.contramap(base)(identity)

  given CanEqual[NodeId, NodeId] = CanEqual.derived