package com.stoufexis.raft.model

import com.stoufexis.raft.typeclass.IntLike

opaque type Term = Long

object Term:
  extension (n: Term)
    infix def long: Long = n

  def init: Term = 1

  infix def apply(l: Long): Term = l

  given IntLike[Term]        = IntLike.IntLikeLong
  given CanEqual[Term, Term] = CanEqual.derived
