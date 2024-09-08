package com.stoufexis.raft.model

import doobie.util.*

import com.stoufexis.raft.typeclass.IntLike

opaque type Term = Int

object Term:
  def init: Term = 1

  given IntLike[Term]        = IntLike.IntLikeInt
  given CanEqual[Term, Term] = CanEqual.derived
  given Get[Term]            = Get[Int]
  given Put[Term]            = Put[Int]
