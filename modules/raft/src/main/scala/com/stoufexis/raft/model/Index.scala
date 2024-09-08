package com.stoufexis.raft.model

import com.stoufexis.raft.typeclass.IntLike
import doobie.util.*

opaque type Index = Int

object Index:
  inline def init: Index = 0

  inline def unsafe(i: Int): Index = i

  given IntLike[Index]         = IntLike.IntLikeInt
  given CanEqual[Index, Index] = CanEqual.derived
  given Get[Index]             = Get[Int]
  given Put[Index]             = Put[Int]
