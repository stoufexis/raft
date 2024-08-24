package com.stoufexis.leader.model

import com.stoufexis.leader.typeclass.Counter

opaque type Term = Int

object Term:
  given Counter[Term]     = Counter.IncreasingInt
  given CanEqual[Term, Term] = CanEqual.derived
