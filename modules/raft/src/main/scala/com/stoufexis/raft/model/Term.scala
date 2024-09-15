package com.stoufexis.raft.model

import com.stoufexis.raft.typeclass.IntLike

opaque type Term = Long

object Term:
  def init: Term = 1

  given IntLike[Term]        = IntLike.IntLikeLong
  given CanEqual[Term, Term] = CanEqual.derived
