package com.stoufexis.leader.model

import com.stoufexis.leader.typeclass.IntLike

opaque type Term = Int

object Term:
  given IntLike[Term]        = IntLike.IntLikeInt
  given CanEqual[Term, Term] = CanEqual.derived
