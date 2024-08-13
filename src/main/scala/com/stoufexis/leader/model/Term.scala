package com.stoufexis.leader.model

opaque type Term = Int

object Term:
  def init: Term = 1

  given Ordering[Term] = Ordering.Int
  given CanEqual[Term, Term] = CanEqual.derived
