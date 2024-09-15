package com.stoufexis.raft.model

import cats.*

opaque type CommandId = String

object CommandId:
  extension (n: CommandId)
    infix def string: String = n

  infix def apply(str: String): CommandId = str

  inline def deriveFunctor[F[_]](using F: Functor[F], base: F[String]): F[CommandId] =
    F.map(base)(identity)

  inline def deriveContravariant[F[_]](using F: Contravariant[F],base: F[String]): F[CommandId] =
    F.contramap(base)(identity)
