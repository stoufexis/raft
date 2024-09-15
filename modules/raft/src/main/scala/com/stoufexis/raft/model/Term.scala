package com.stoufexis.raft.model

import doobie.util.*

import com.stoufexis.raft.typeclass.IntLike

opaque type Term = Long

object Term:
  extension (n: Term)
    infix def long: Long = n

  inline def uninitiated: Term = 0
  inline def init:        Term = 1

  infix def apply(l: Long): Term = l

  given IntLike[Term]        = IntLike.IntLikeLong
  given CanEqual[Term, Term] = CanEqual.derived
  given Put[Term]            = Put[Long]
  given Get[Term]            = Get[Long]
