package com.stoufexis.leader.model

import com.stoufexis.leader.typeclass.Increasing

opaque type Term = Int

object Term:
  given Increasing[Term]     = Increasing.IncreasingInt
  given CanEqual[Term, Term] = CanEqual.derived
