package com.stoufexis.leader.model

opaque type Term = Int

object Term:
  def init: Term = 1

  extension (term: Term)
    def next: Term = term + 1
    def toInt: Int = term

  given Ordering[Term] = Ordering.Int
  given CanEqual[Term, Term] = CanEqual.derived
