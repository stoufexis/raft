package com.stoufexis.leader.model

import com.stoufexis.leader.typeclass.IntLike

opaque type Index = Int

object Index:
  def init: Index = 0

  given IntLike[Index]         = IntLike.IntLikeInt
  given CanEqual[Index, Index] = CanEqual.derived
