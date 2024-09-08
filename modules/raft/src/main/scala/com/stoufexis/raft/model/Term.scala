package com.stoufexis.raft.model

import doobie.util.*

import com.stoufexis.raft.typeclass.IntLike

opaque type Term = Int

object Term:
  given IntLike[Term]        = IntLike.IntLikeInt
  given CanEqual[Term, Term] = CanEqual.derived
  given Get[Term]            = Get[Int]
  given Put[Term]            = Put[Int]
