package com.stoufexis.raft.model

import com.stoufexis.raft.typeclass.IntLike

opaque type Index = Long

object Index:
  inline def init: Index = 1

  inline def unsafe(i: Int): Index = i

  given IntLike[Index]         = IntLike.IntLikeLong
  given CanEqual[Index, Index] = CanEqual.derived
