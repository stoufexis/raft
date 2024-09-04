package com.stoufexis.raft.model

import com.stoufexis.raft.typeclass.IntLike

opaque type Term = Int

object Term:
  given IntLike[Term]        = IntLike.IntLikeInt
  given CanEqual[Term, Term] = CanEqual.derived
